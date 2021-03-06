import requests
import json
import logging
import argparse
from datetime import datetime, timedelta

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

#Function to fetch the API data parallely with a thread for every 100000 records
class FetchAPIData(beam.DoFn):
    def __init__(self, api_url, app_token,query_end_date):
        self.api_url=api_url
        self.apptoken=app_token
        self.query_end_date=query_end_date
        
    def process(self, element):
        import requests
        import json
        
        if self.query_end_date:
            query=f"""$where=:created_at>='{self.query_end_date}' OR :updated_at>='{self.query_end_date}'&$order=:id&$limit=10000&$offset={element}"""
            api_response=requests.get(f"{self.api_url}?{query}",
                headers={'X-App-Token':self.apptoken})
            api_response=json.loads(api_response.content)
            return api_response
        
        else:
            query=f"""$order=:id&$limit=10000&$offset={element}"""
            api_response=requests.get(f"{self.api_url}?{query}",
                headers={'X-App-Token':self.apptoken})
            api_response=json.loads(api_response.content)
            return api_response

def run(argv=None):
    """
    Pipeline for reading data from a Socratra Open API and 
    writing the results to Google Cloud Storage
    
    Args:
        api-url         : SODA API base url
        app-token       : SODA API App Token
        output-bucket   : Ouput GCS bucket name
        api-name        : API Name is used as the folder and file name
        execution-date  : date in which the job runs
        days-ago        : No of days of data to be fetched. Blank indicates full-load
    """
    parser=argparse.ArgumentParser()
    parser.add_argument('--api-url',
                        dest='api_url',
                        default='https://data.cityofnewyork.us/resource/6z8x-wfk4.json',
                        help='SODA API base url')
    parser.add_argument('--app-token',
                        dest='app_token',
                        default='l0AFei7KQOANOi9RpP3XuO3jo',
                        help='SODA API App Token')
    parser.add_argument('--output-bucket',
                        dest='output_bucket',
                        default='output',
                        help='Ouput GCS bucket name')
    parser.add_argument('--api-name',
                        dest='api_name',
                        default='evictions',
                        help='API Name is used as the folder and file name')
    parser.add_argument('--execution-date',
                        dest='execution_date',
                        default='2022-04-05',
                        help='date in which the job runs')
    parser.add_argument('--days-ago',
                        dest='days_ago',
                        default=None,
                        help='No of days of data to be fetched. Blank indicates full-load')
    known_args, pipeline_args=parser.parse_known_args(argv)

    pipeline_options=PipelineOptions(pipeline_args)

    #parse input arguments
    app_token=known_args.app_token
    api_url=known_args.api_url
    output_bucket=known_args.output_bucket
    api_name=known_args.api_name
    execution_date=datetime.strptime(known_args.execution_date,'%Y-%m-%d')
    days_ago=int(known_args.days_ago) if known_args.days_ago else None
    
    #fetch the number of records in the API
    if days_ago:
        query_end_date=execution_date + timedelta(days=-(days_ago))
        query_end_date=datetime.strftime(query_end_date,'%Y-%m-%d')
        r=requests.get(f"""{api_url}?$select=count(*)&$where= (:created_at>='{query_end_date}' OR :updated_at>='{query_end_date}')""",
            headers={'X-App-Token':app_token})
    else:
        query_end_date=None
        r=requests.get(f"{api_url}?$select=count(*)",
            headers={'X-App-Token':app_token})
    
    record_count=int(json.loads(r.content)[0]['count'])
    
    #range to parallelize the api call for every 100000 records
    api_range=list(range(0,record_count,100000))

    with beam.Pipeline(options=pipeline_options) as p:

        input_rows=(p
                    | 'DefineRange' >> beam.Create(api_range)
                    | 'FetchAPIData' >> beam.ParDo(FetchAPIData(api_url,app_token,query_end_date))
                    | 'WriteToGCS' >> beam.io.WriteToText(f'{output_bucket}/{api_name}/{execution_date.year}/{execution_date.month}/{execution_date.day}/{api_name}', file_name_suffix='.json')
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()