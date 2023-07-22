# Only certain packages necessary for DAG relations will get top-level imports
# See the reasoning here: https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#top-level-python-code
import pendulum

# We will write the DAG using the TaskFlow API introduced in Airflow 2.0
from airflow.decorators import dag, task


@dag(
    schedule = '@hourly',
    start_date = pendulum.datetime(2022, 12, 18, tz = 'Asia/Jakarta'),
    catchup = False
)
def bpi_etl_bigquery():

    @task(multiple_outputs = True)
    def extract_bpi():
    
        import requests, json, hashlib
        from google.cloud import storage as gcp_storage
        
        # We will store several details that the next tasks will need
        # to locate the file created in this task
        return_dict = {}
        
        
        # This will be useful for the exchange rate API, so we store it first here
        run_timestamp = pendulum.now()
        return_dict['run_timestamp'] = run_timestamp.to_datetime_string()
        
        
        # Get and store data from the CoinDesk BPI API, and store the JSON filename
        return_dict['extract_file'] = 'bpi-raw-data.json'
        
        bpi_req = requests.get('https://api.coindesk.com/v1/bpi/currentprice.json')
        bpi_json = bpi_req.json()
        
        with open(return_dict['extract_file'], 'w') as file:
            json.dump(bpi_json, file)
        
        
        # Upload our saved JSON to Google Cloud Storage (snapshotting raw data)
        
        # A random prefix when using GCS is great to help GCP autoscale our requests
        # See https://cloud.google.com/storage/docs/request-rate#naming-convention0
        with open(return_dict['extract_file'], 'rb') as file:
            md5_prefix = hashlib.md5(file.read()).hexdigest()[0:6]
            
        timestamp_prefix = run_timestamp.strftime('%Y-%m-%d-%H-%M-%S')
        complete_prefix = f'{md5_prefix}-{timestamp_prefix}'
        
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
    
        from airflow.models import Variable
        
        import requests, json, hashlib
        from google.cloud import storage as gcp_storage
        
        fetch_datetime = pendulum.parse(fetch_timestamp)

        # We will store several details that the next tasks will need
        # to locate the file created in this task
        return_dict = {}
        
        
        # Get and store data from the Open Exchange Rates API, and store the JSON filename
        return_dict['extract_file'] = 'rupiah-exchange-rate.json'
        
        xr_fetch_date = fetch_datetime.strftime('%Y-%m-%d')

        auth_params = {
            'app_id': Variable.get('oer_api_key'),
            'symbols': 'IDR'
        }

        IDR_xr_url = f'https://openexchangerates.org/api/historical/{xr_fetch_date}.json'
        IDR_xr_req = requests.get(IDR_xr_url, params = auth_params)

        IDR_xr_json = IDR_xr_req.json()

        with open(return_dict['extract_file'], 'w') as file:
            json.dump(IDR_xr_json, file)
        
        
        # Upload our saved JSON to Google Cloud Storage (snapshotting raw data)
        
        # A random prefix when using GCS is great to help GCP autoscale our requests
        # See https://cloud.google.com/storage/docs/request-rate#naming-convention0
        with open(return_dict['extract_file'], 'rb') as file:
            md5_prefix = hashlib.md5(file.read()).hexdigest()[0:6]
            
        timestamp_prefix = fetch_datetime.strftime('%Y-%m-%d-%H-%M-%S')
        complete_prefix = f'{md5_prefix}-{timestamp_prefix}'
        
        # Must be stored to ensure the next tasks know where to find it!
        return_dict['gcs_dest'] = f"data/raw/{complete_prefix}/{return_dict['extract_file']}"
        return_dict['gcs_bucket'] = '371516-bpi-etl'
        
        # Save to Google Cloud Storage
        gcs_client = gcp_storage.Client()
        bucket = gcs_client.bucket(return_dict['gcs_bucket'])
        blob_object = bucket.blob(return_dict['gcs_dest'])
        
        blob_object.upload_from_filename(f"./{return_dict['extract_file']}")
        
        return return_dict
    
    bpi_data_info = extract_bpi()
    xr_data_loc = extract_xr(bpi_data_info['run_timestamp'])

bpi_etl_bigquery()