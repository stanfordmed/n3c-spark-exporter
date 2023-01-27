#!/usr/bin/env python3

import sys
from google.cloud import storage
import argparse
import os
from pathlib import Path
import yaml
from google.cloud import dataproc_v1
from datetime import date
import traceback
import logging

#
# This class should be initialized with a config.yaml file similar to config_example.yaml
# The trigger_batch method triggers pyspark batch to run on cloud and after a successful run of the batch, it consolidates the csv parts
#
# TODO - serialize the config file into a bean
#
spark_bq_jar = 'gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.27.1.jar'
gs_bucket_config = 'gcs_bucket'
prefix_config = 'prefix'
cdm_tables_config = 'cdm_tables'
data_counts_folder = 'data_counts'
mainfest = 'manifest'
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
file_handler = logging.FileHandler('n3c_spark_extractor.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

class n3c_spark_extractor:
  env_config : any
  batch_config : any
  gcs_bucket : str
  prefix : str
  script_file_in_bucket : str = 'scripts/spark_sql_batch.py'
  blobs_to_delete : list
  
  def __init__(self, env_config_on_disk, script_file_on_disk, batch_config_on_disk):
    with open(env_config_on_disk, "r") as f:
      self.env_config = yaml.safe_load(f)
      self.gcs_bucket = self.env_config[gs_bucket_config]
      self.prefix = self.env_config[prefix_config]

      with open(batch_config_on_disk, "r") as bf:
        self.batch_config = yaml.safe_load(bf)

      # copy the pyspark batch script to gcs
      if self.prefix == None:
        self.prefix = date.today().strftime("%b-%d-%Y")
        logger.info('No prefix supplied, using - {self.prefix}')
      self.script_file = f'{self.prefix}/{self.script_file_in_bucket }'
      storage_client = storage.Client()
      bucket = storage_client.get_bucket(self.gcs_bucket)
      self.script_file_in_bucket = f'{self.prefix}/{self.script_file_in_bucket}'
      blob = bucket.blob(self.script_file_in_bucket)
      logger.info(f'Copying spark_sql_batch.py file to gs://{self.gcs_bucket}/{self.script_file_in_bucket }') 
      blob.upload_from_filename(script_file_on_disk)
      self.blobs_to_delete = []
      self.blobs_to_delete.append(blob)

      # copy the config files to gcs         
      config_blob1 = bucket.blob(f'{self.prefix}/batch_config.yaml')
      logger.info(f'Copying config file to gs://{self.gcs_bucket}/batch_config.yaml')
      config_blob1.upload_from_filename(batch_config_on_disk)
      self.blobs_to_delete.append(config_blob1)

      logger.info(f'Copying config file to gs://{self.gcs_bucket}/env_config.yaml')
      config_blob2 = bucket.blob(f'{self.prefix}/env_config.yaml')
      config_blob2.upload_from_filename(env_config_on_disk)
      self.blobs_to_delete.append(config_blob2)
            
  def extract(self):
    cdm_tables = self.batch_config[cdm_tables_config]
    cdm_table_list : list = cdm_tables.split(",")
    logger.info(f'Processing {cdm_table_list}')
    
    try:
      # trigger pyspark batch process
      result = self.trigger_batch()

      # compose csvs for each table
      for table in cdm_table_list:
        self.compose_csvs(table.strip())
      self.compose_csvs('data_counts')

      cdm_table_list.append(mainfest)
      cdm_table_list.append(data_counts_folder)
      self.download_csvs(cdm_table_list)
    except Exception as e:
      logger.error(f'Error while trigeering with job, continuing with cleanup')
      logger.critical(e, exc_info=True)
    finally:
      # cleanup files from bucket/prefix
      self.delete_csvs(data_counts_folder)
      self.delete_csvs(mainfest)
      for table in cdm_table_list:
        self.delete_csvs(table.strip())
      # cleanup files we saved to bucket
      for blob in self.blobs_to_delete:
        logger.info(f'Deleting {blob.name}')
        try:
          blob.delete()
        except Exception as e:
          logger.error(f'Error while deleting {blob.name}, continuing with cleanup') 
        
  def trigger_batch(self):
    from google.cloud import dataproc_v1
    logger.info('Triggering pyspark batch')
    service_account = self.env_config['service_account']
    location = self.env_config['region']
    subnetwork_uri = self.env_config['subnetwork_uri']

    client = dataproc_v1.BatchControllerClient( client_options={"api_endpoint": "{}-dataproc.googleapis.com:443".format(location)})
        
    # Initialize request argument(s)
    batch = dataproc_v1.Batch()
    batch.pyspark_batch.main_python_file_uri = f'gs://{self.gcs_bucket}/{self.script_file_in_bucket}'
    batch.pyspark_batch.jar_file_uris = [spark_bq_jar] 
    batch.pyspark_batch.file_uris = [f'gs://{self.gcs_bucket}/{self.prefix}/batch_config.yaml', f'gs://{self.gcs_bucket}/{self.prefix}/env_config.yaml']

    # Pass all the arguments you want to use in the spark job
    batch.pyspark_batch.args = ['--batch_config', 'batch_config.yaml', 
      '--env_config', 'env_config.yaml']
    environmentConfig = dataproc_v1.EnvironmentConfig()
    executionConfig = dataproc_v1.ExecutionConfig()
    executionConfig.service_account = service_account
    executionConfig.subnetwork_uri = subnetwork_uri
    environmentConfig.execution_config = executionConfig
    batch.environment_config = environmentConfig

    request = dataproc_v1.CreateBatchRequest(
      parent=f"projects/{self.env_config['project_id']}/locations/{self.env_config['region']}",
         batch=batch,
      )

    # Make a request to create a batch
    operation = client.create_batch(request=request)
    logger.info('Waiting for operation to complete, this may take a while...')
    wait : int = self.env_config['timeout_in_min'] * 60
    result = operation.result(timeout=wait)
    logger.info(result)
    
  def delete_csvs(self, cdm_table_name) : 
    logger.info(f'Checking if {cdm_table_name} exists in bucket {self.gcs_bucket}') 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(self.gcs_bucket)
    cdm_table_name_upper = cdm_table_name.upper()
    for page in bucket.list_blobs().pages:
      for blob in page:
        if blob.name.startswith(f'{self.prefix}') and cdm_table_name in blob.name.upper(): 
          logger.info(f'Deleting {blob.name}')
          try:
            blob.delete()
          except Exception as e:
            logger.info(f'Error while deleting {blob.name}')
      
  def compose_csvs(self, cdm_table_name) :
    logger.info(f'Composing csv parts for {cdm_table_name}')
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(self.gcs_bucket)
    path = cdm_table_name
    if self.prefix != None :
      path = f'{self.prefix}/{cdm_table_name}'
    segments = []

    # keep the header file as first element in the segements
    for page in bucket.list_blobs().pages:
      for blob in page:
        if blob.name.startswith(f'{path}/'):
          if blob.name.endswith('header.csv') :
            segments.append(blob)
            break

    # keep all the remaining csv part files in the segments following the header file
    for page in bucket.list_blobs().pages:
      for blob in page:
        if blob.name.startswith(f'{path}/'):
          if blob.name.endswith('.csv') and blob.name.endswith('header.csv') != True:
            segments.append(blob)
        
    for segment in segments:
      logger.info ('Adding file - ' + segment.name)
    cdm_table_upper = cdm_table_name.upper()
    destination = bucket.blob(f'{self.prefix}/{cdm_table_upper}.csv')

    # compose allows cmposing 32 files in one go
    if len(segments) <= 32:
      destination.compose(segments)
    else:
      # Requires iterative composition for every 32 files
      compositions = []
      windex = 0
      for i, b in enumerate(segments):
        if i != 0 and i % 32 == 0:
          next_blob = bucket.blob("{}-tmp-{:02d}-{:02d}.csv".format(self.prefix[:-1], windex, i - 1))
          next_blob.content_type = "text/plain"
          next_blob.compose(segments[windex:i])
          compositions.append(next_blob)
          logger.info("Composed from %s-%s -> %s", windex, i - 1, next_blob.name)
          windex = i
      if windex < len(segments):
        next_blob = bucket.blob("{}-tmp-{:02d}-{:02d}.csv".format(self.prefix[:-1], windex, len(segments)))
        next_blob.content_type = "text/plain"
        next_blob.compose(segments[windex:len(segments)])
        compositions.append(next_blob)
        logger.info("Composed from %s-%s -> %s (final)", windex, len(segments), next_blob.name)
        destination.compose(compositions)

      # Delete intermediate compositions
      for composition in compositions:
        composition.delete()
    # Delete the temporary header file and all part files
    for segment in segments :
      segment.delete()
    logger.info(f'Done composing csv parts for {cdm_table_name}')

  def download_csvs(self, cdm_table_list):
    logger.info('Downloading csvs...')
    # check if output folder exists, if not, create one
    output_dir = self.env_config['output_dir']
    datafiles_dir = f'{output_dir}/DATAFILES'
    if os.path.exists(output_dir):
      if os.path.exists(datafiles_dir) == False:
        os.mkdir(datafiles_dir)
    else:  
      os.mkdir(output_dir)
      os.mkdir(datafiles_dir)

    # download the cdm_table csv files, MANIFEST.csv and DATA_COUNTS.csv 
    for table in cdm_table_list:
      cdm_table = table.strip()
      cdm_table_name_upper = cdm_table.upper()
      cdm_table_name_upper = f'{cdm_table_name_upper}.csv'
      logger.info(f'Downaloading {cdm_table_name_upper} from gcs bucket {self.gcs_bucket}') 
      
      storage_client = storage.Client()
      bucket = storage_client.get_bucket(self.gcs_bucket)

      for page in bucket.list_blobs().pages:
        for blob in page:
          if blob.name.startswith(f'{self.prefix}') and cdm_table_name_upper in blob.name and data_counts_folder not in blob.name: 
            logger.info(f'Downlaoding {blob.name}')
            output_folder = output_dir
            if cdm_table != mainfest and cdm_table != data_counts_folder:
              output_folder = datafiles_dir
            file = f'{output_folder}/{cdm_table_name_upper}'
            content = bucket.blob(blob.name)
            content.download_to_filename(file)
            logger.info(f'Downaloaded {output_dir}{cdm_table_name_upper}')
      logger.info('Done downloading csvs...')
      