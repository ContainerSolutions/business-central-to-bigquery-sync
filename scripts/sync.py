import requests
from requests.auth import HTTPBasicAuth
from apache_beam.io.gcp.bigquery import parse_table_schema_from_json
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.internal.clients import bigquery
from datetime import datetime
import dateutil.parser
import apache_beam as beam
import pandas as pd
import json
import copy
import os
import urllib
import pprint
import argparse
import logging
import traceback


# AUTH MODE – LOGIN\PASSWORD (USERS AND WEB SERVICES KEY)
USERNAME = os.getenv("USERNAME")  # username
SERVICE_KEY = os.getenv("SERVICE_KEY")   # Service KEY
BASE_URL = os.getenv("BASE_URL", "https://api.businesscentral.dynamics.com/v2.0")
ENDPOINT = os.getenv("ENDPOINT")
SERVICE = os.getenv("SERVICE")
COMPANIES = os.getenv("COMPANIES", "")
DATE_FIELDS=os.getenv("DATE_FIELDS", "")
DATE_TIME_FIELDS=os.getenv("DATE_TIME_FIELDS", "")
SYNC_BUCKET=os.getenv("SYNC_BUCKET")

def main(argv=None):
    """
    The main function which creates the pipeline and runs it.
    """
    parser = argparse.ArgumentParser()
    # Here we add some specific command line arguments we expect.
    # This defaults the output table in your BigQuery you'll have
    # to create the example_data dataset yourself using bq mk temp
    parser.add_argument('--output',
                        dest='output',
                        required=False)

    # Parse arguments from the command line.
    known_args, pipeline_args = parser.parse_known_args(argv)

    table_spec = bigquery.TableReference(
        projectId=os.getenv('GOOGLE_PROJECT_ID'),
        datasetId=os.getenv('GOOGLE_DATASET_ID'),
        tableId=os.getenv('GOOGLE_TABLE_ID')
    )

    pipeline_options = PipelineOptions(
        flags=pipeline_args,
    )
    pipeline = beam.Pipeline(options=pipeline_options)

    (pipeline
     | 'Get Data From Business Central ' >> beam.Create(get_data_from_bc365())
     | 'Format Dates' >> beam.ParDo(FormatElements())
     | 'Write To BigQuery' >> beam.io.WriteToBigQuery(
         table_spec,
         write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
         create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
         custom_gcs_temp_location=SYNC_BUCKET)
     )
    pipeline.run().wait_until_finish()



class FormatElements(beam.DoFn):
    def __init__(self):
        pass

    def process(self, element):
        try:
            del element["@odata.etag"]
        except KeyError:
            pass
        remove_list = []
        element_copy = copy.deepcopy(element)
        for key in element_copy:
            if "_" in key:
                remove_list.append(key)
                new_key = key.replace("_", "")
                element[new_key] = element[key]
        for key in remove_list:
            del element[key]
        for key in element:
            
            if key in get_date_fields():
                value = element.get(key)
                element[key] = datetime.fromisoformat(value)
            if key in get_datetime_fields():
                value = element.get(key)
                element[key] = dateutil.parser.parse(value)
                element[key] = element[key].replace(tzinfo=None)
                element[key] = element[key].replace(microsecond=0)
        return [element]


def get_data_from_bc365():
    data = []
    for company in get_companies():
        res = requests.get(
            get_url(company),
            auth=HTTPBasicAuth(USERNAME, SERVICE_KEY),
            headers={
                "OData-MaxVersion": "4.0",
                "OData-Version": "4.0",
                "Accept": "application/json",
                "Content-Type": "application/json; charset=utf-8",
                "Prefer": "odata.include-annotations=OData.Community.Display.V1.FormattedValue"
            }
        )
        res_data = res.json().get("value")
        df = pd.DataFrame.from_dict(res_data)
        df["Company"] = company
        if len(list(df.T.to_dict().values())) > 0:
            data = data + list(df.T.to_dict().values())
    return data

def get_url(company):
    return f"{BASE_URL}/{ENDPOINT}/Company('{urllib.parse.quote(company)}')/{SERVICE}"

def get_companies():
    return COMPANIES.split(",")

def get_date_fields():
    return DATE_FIELDS.split(",")

def get_datetime_fields():
    return DATE_TIME_FIELDS.split(",")

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    main()
