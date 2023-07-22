# Only certain packages necessary for DAG relations will get top-level imports
# See the reasoning here: https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#top-level-python-code
import pendulum

# We will write the DAG using the TaskFlow API introduced in Airflow 2.0
from airflow.decorators import dag, task

def create_random_dt_prefix(timestamp, filename):
    """
    Create a random prefix using MD5 hash on top of a datetime-based prefix for Google Cloud Storage objects
    
    A random prefix when using GCS is great to help GCP autoscale our requests
    See more here: https://cloud.google.com/storage/docs/request-rate#naming-convention0
    """
    
    import hashlib
    
    with open(filename, 'rb') as file:
        md5_prefix = hashlib.md5(file.read()).hexdigest()[0:6]
            
    timestamp_prefix = timestamp.strftime('%Y-%m-%d-%H-%M-%S')
    
    return f'{md5_prefix}-{timestamp_prefix}'
        
    
@dag(
    schedule = '@hourly',
    start_date = pendulum.datetime(2022, 12, 18, tz = 'Asia/Jakarta'),
    catchup = False
)
def bpi_etl_bigquery():
    """
    # Bitcoin Price Index - ETL to BigQuery
    Extract data from [CoinDesk's Bitcoin Price Index (BPI) API](https://api.coindesk.com/v1/bpi/currentprice.json), clean and transform while enriching with IDR exchange rate from [Open Exchange Rates API](https://docs.openexchangerates.org/reference/api-introduction), load into BigQuery (frequency = hourly).
    """
    
    @task(multiple_outputs = True)
    def extract_bpi():
        """
        ## Extract BPI
        Extract Bitcoin Price Index containing conversion rates of Bitcoin with USD, GBP, and EUR from [CoinDesk's API](https://api.coindesk.com/v1/bpi/currentprice.json), and save the raw JSON on Google Cloud Storage.
        """
        
        import requests, json
        from google.cloud import storage as gcp_storage
        
        from bpi_etl.common_module.pydantic_models import BPI
        
        # We will store several details that the next tasks will need
        # to locate the file created in this task
        return_dict = {}
        
        
        # This will be useful for the exchange rate API, so we store it first here
        run_timestamp = pendulum.now()
        return_dict['run_timestamp'] = run_timestamp.to_datetime_string()
        
        
        # Get, validate, and store data from the CoinDesk BPI API, and store the JSON filename
        return_dict['extract_file'] = 'bpi-raw-data.json'
        
        bpi_req = requests.get('https://api.coindesk.com/v1/bpi/currentprice.json')
        bpi_json = bpi_req.json()
        
        # Validation with Pydantic
        bpi_dict = BPI(**bpi_json).dict()
        
        with open(return_dict['extract_file'], 'w') as file:
            json.dump(bpi_dict, file)
        
        
        # Upload our saved JSON to Google Cloud Storage (snapshotting raw data)
        
        complete_prefix = create_random_dt_prefix(run_timestamp, return_dict['extract_file'])
        
        # Must be stored to ensure the next tasks know where to find it!
        return_dict['gcs_dest'] = f"data/raw/{complete_prefix}/{return_dict['extract_file']}"
        return_dict['gcs_bucket'] = '371516-bpi-etl'
        
        # Save to Google Cloud Storage
        gcs_client = gcp_storage.Client()
        bucket = gcs_client.bucket(return_dict['gcs_bucket'])
        blob_object = bucket.blob(return_dict['gcs_dest'])
        
        blob_object.upload_from_filename(f"./{return_dict['extract_file']}")
        
        return return_dict
    
    # XR is (well-)known as a shorthand for eXchange Rate
    @task(multiple_outputs = True)
    def extract_xr(fetch_timestamp):
        """
        ## Extract IDR Exchange Rate (XR)
        Extract the latest, hourly USD-IDR exchange rate from [Open Exchange Rates API](https://docs.openexchangerates.org/reference/api-introduction), to be used to enrich BPI data later, and save the raw JSON on Google Cloud Storage.
        """
        
        from airflow.models import Variable
        
        import requests, json
        from google.cloud import storage as gcp_storage
        
        from bpi_etl.common_module.pydantic_models import HistoricalXR
        
        fetch_datetime = pendulum.parse(fetch_timestamp)

        # We will store several details that the next tasks will need
        # to locate the file created in this task
        return_dict = {}
        
        
        # Get, validate, and store data from the Open Exchange Rates API, and store the JSON filename
        return_dict['extract_file'] = 'rupiah-exchange-rate.json'
        
        xr_fetch_date = fetch_datetime.strftime('%Y-%m-%d')

        auth_params = {
            'app_id': Variable.get('oer_api_key'),
            'symbols': 'IDR'
        }
        
        # Despite using the historical API, actually it has identical content
        # if we call the API during the day queried
        IDR_xr_url = f'https://openexchangerates.org/api/historical/{xr_fetch_date}.json'
        IDR_xr_req = requests.get(IDR_xr_url, params = auth_params)

        IDR_xr_json = IDR_xr_req.json()
        
        # Validation with Pydantic
        IDR_xr_dict = HistoricalXR(**IDR_xr_json).dict()
        
        with open(return_dict['extract_file'], 'w') as file:
            json.dump(IDR_xr_dict, file)
        
        
        # Upload our saved JSON to Google Cloud Storage (snapshotting raw data)
        
        complete_prefix = create_random_dt_prefix(pendulum.now(), return_dict['extract_file'])
        
        # Must be stored to ensure the next tasks know where to find it!
        return_dict['gcs_dest'] = f"data/raw/{complete_prefix}/{return_dict['extract_file']}"
        return_dict['gcs_bucket'] = '371516-bpi-etl'
        
        # Save to Google Cloud Storage
        gcs_client = gcp_storage.Client()
        bucket = gcs_client.bucket(return_dict['gcs_bucket'])
        blob_object = bucket.blob(return_dict['gcs_dest'])
        
        blob_object.upload_from_filename(f"./{return_dict['extract_file']}")
        
        return return_dict
    
    @task(multiple_outputs = True)
    def transform_data(bpi_data_loc, xr_data_loc):
        """
        ## Transform Data
        Clean, transform, and enrich BPI data with additional IDR conversion rate, given input of all the raw data necessary, and save as a Parquet file on Google Cloud Storage, prepared for loading into BigQuery later 
        """
        
        import json
        import pandas as pd
        
        from google.cloud import storage as gcp_storage
        
        gcs_client = gcp_storage.Client()
        
        # We will store several details that the next tasks will need
        # to locate the file created in this task
        return_dict = {}
        
        
        # Read our raw data from GCS into dictionaries 
        
        for file_loc in [bpi_data_loc, xr_data_loc]: 
            bucket = gcs_client.bucket(file_loc['gcs_bucket'])
            blob_object = bucket.blob(file_loc['gcs_dest'])
            
            blob_object.download_to_filename(f"./{file_loc['extract_file']}")

        with open(bpi_data_loc['extract_file'], 'r') as file:
            bpi_json = json.load(file)
            
        with open(xr_data_loc['extract_file'], 'r') as file:
            xr_json = json.load(file)
        
        
        # Convert our BPI JSON into a DataFrame
        bpi_df = pd.json_normalize(bpi_json)
        
        
        # Filter to retain only select desirable columns
        
        # List out only desirable meta information
        final_meta_columns = ['disclaimer', 'chartName', 'time.updated', 'time.updatedISO']

        # List out only desirable currency metadata, for all currencies at once
        final_metadata_regex = '|'.join(['code', 'rate_float', 'description'])
        final_currency_filter = bpi_df.columns.\
            str.contains(f'^bpi\\.(?:.*).{final_metadata_regex}$', regex = True)
        final_currency_columns = bpi_df.columns[final_currency_filter].tolist()

        # Combine list of desirable variables and filter our DataFrame
        final_columns = final_meta_columns + final_currency_columns

        bpi_df = bpi_df.loc[:, bpi_df.columns.isin(final_columns)]
        
        
        # Change variable names from camel case to snake case # TO DO: is this true?
        bpi_df.columns = bpi_df.columns.\
        str.replace('.', '_', regex = False).\
        str.replace('([a-z])([A-Z])', '\\1_\\2', regex = True).\
        str.lower()
        
        
        # Enrich with IDR-BTC exchange rate
        USD_IDR_rate = xr_json['rates']['IDR']
        
        bpi_df['bpi_idr_rate_float'] = bpi_df['bpi_usd_rate_float'] * USD_IDR_rate
        
        
        # Convert datetime format and add last_updated variable
        date_format = '%Y-%m-%d %H:%M:%S'
    
        bpi_df[['time_updated', 'time_updated_iso']] = bpi_df[['time_updated', 'time_updated_iso']].\
            apply(lambda timestamp: pd.to_datetime(timestamp).dt.strftime(date_format))

        bpi_df['last_updated'] = pendulum.now().strftime(date_format)
        
        
        # Save our DataFrame as a parquet file, and store the filename
        # It is VERY important that we mark has_nulls columns in accordance to
        # the table schema we use with BigQuery
        return_dict['extract_file'] = 'bpi-xr-data.parquet'
        
        bpi_df.to_parquet(return_dict['extract_file'], index = False, engine = 'fastparquet', has_nulls = ['disclaimer', 'chart_name'])
        
        
        # Upload our saved parquet to Google Cloud Storage (snapshotting transformed data)
        
        complete_prefix = create_random_dt_prefix(pendulum.now(), return_dict['extract_file'])
        
        # Must be stored to ensure the next task know where to find it!
        return_dict['gcs_dest'] = f"data/bigquery_load/{complete_prefix}/{return_dict['extract_file']}"
        return_dict['gcs_bucket'] = '371516-bpi-etl'
        
        # Save to Google Cloud Storage
        blob_object = bucket.blob(return_dict['gcs_dest'])
        
        blob_object.upload_from_filename(f"./{return_dict['extract_file']}")
        
        return return_dict
    
    @task()
    def load_data(final_data_loc):
        """
        ## Load Data
        Load enriched and cleaned BPI data into BigQuery from a Parquet file saved on Google Cloud Storage previously.
        """
        
        from google.cloud import bigquery
        
        bq_client = bigquery.Client()
        
        job_config = bigquery.LoadJobConfig(
            write_disposition = bigquery.WriteDisposition.WRITE_APPEND,
            source_format = bigquery.SourceFormat.PARQUET
        )
        
        gcs_uri = f"gs://{final_data_loc['gcs_bucket']}/{final_data_loc['gcs_dest']}"
        bq_table_id = 'bitcoin_price_index.bpi_xr_hourly'
        
        load_job = bq_client.load_table_from_uri(
            gcs_uri, bq_table_id, job_config = job_config
        )
            
        load_job.result()
    
    bpi_data_info = extract_bpi()
    xr_data_loc = extract_xr(bpi_data_info['run_timestamp'])
    
    final_data_loc = transform_data(bpi_data_info, xr_data_loc)
    
    load_data(final_data_loc)
    
    
bpi_etl_bigquery()