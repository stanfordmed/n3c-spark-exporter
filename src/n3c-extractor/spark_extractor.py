#!/usr/bin/env python3

import asyncio
import sys, getopt
from google.cloud import storage
import argparse
import os
from pathlib import Path
import yaml
import time
import time
import csv

spark_bq_jar = 'gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.27.1.jar'
gs_bucket_config = 'gcs_bucket'
prefix_config = 'prefix'
cdm_tables_config = 'cdm_tables'
data_counts_folder = 'data_counts'

timestamp_format = 'yyyy-MM-dd HH:mm:ss'
mainfest = 'manifest'

class SparkSqlBatch:
  spark : any
  config : any
  gcs_bucket : str
  prefix : str
  script_file : str
  data_count_str : str

  def __init__(self, config_file):
    from pyspark.sql import SparkSession
    with open(config_file, "r") as f:      
      # create spark session and set the temp bucket
      self.spark = SparkSession.builder.appName('N3C_extract_runner').config('spark.jars', spark_bq_jar).getOrCreate()
      self.config = yaml.safe_load(f)
      self.gcs_bucket = self.config[gs_bucket_config]
      self.spark.conf.set('temporaryGcsBucket', self.gcs_bucket)
      self.prefix = self.config[prefix_config]

      # create MANIFEST.csv and parts for DATA_COUNTS.csv
      self.run_spark_sql_manifest()
      self.create_data_count_header()

      # extract cdm_tables from subset(persist_database_schema) dataset as csv part files
      dataset_id = self.config['persist_database_schema']
      cdm_tables = self.config[cdm_tables_config].strip()
      cdm_table_list = cdm_tables.split(",")
      for table in cdm_table_list:
        if table in self.config:
          cdm_table_name = table.strip()          
          self.run_spark_sql(dataset_id, cdm_table_name)
        
  def run_spark_sql(self, dataset_id, cdm_table_name):
    print(f'Starting extraction of {cdm_table_name}')

    # execute the query and dump the csv parts for the cdm_table to gcs bucket
    sql = self.config[cdm_table_name]
    cdm_table = self.spark.read.format('bigquery').option('table', f'{dataset_id}.{cdm_table_name}').load()
    cdm_table.createOrReplaceTempView(cdm_table_name) 
    df = self.spark.sql(sql)
    df.printSchema()
    csv_folder = cdm_table_name
    if self.prefix != None:
      csv_folder = f'{self.prefix}/{cdm_table_name}'
    df.write.csv(f'gs://{self.gcs_bucket}/{csv_folder}', sep='|', quote='"', quoteAll=True,  timestampFormat='yyyy-MM-dd HH:mm:ss')

    # create header file for the cdm table
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(self.gcs_bucket)
    blob = bucket.blob(f'{csv_folder}/header.csv')
    columns = []
    for field in df.schema.fields:
      columns.append('"' + field.name + '"')
    blob.upload_from_string('|'.join(columns) + '\n')

    # execute row_count query and dump the results to a csv file in data_counts folder
    rc_foler = data_counts_folder
    if self.prefix != None:
      rc_foler = f'{self.prefix}/{rc_foler}'
    table_name_upper = cdm_table_name.upper()
    rc_sql = self.config['data_count_sql'].format(table_name_upper = table_name_upper, table_name = cdm_table_name)
    rc_df = self.spark.sql(rc_sql)
    rc_df.toPandas().to_csv(f'gs://{self.gcs_bucket}/{rc_foler}/{table_name_upper}.csv', header = False, index = False, sep='|', quotechar='"', quoting=csv.QUOTE_ALL)    

    print(f'Done extraction {cdm_table_name}')

  def run_spark_sql_manifest(self):
    print(f'Extracting MINFEST.csv file')

    dataset_id = self.config['results_database_schema']
    sql = self.config[mainfest].format(site_abbrev = self.config['site_abbrev'],
      site_name = self.config['site_name'],
      contact_name = self.config['contact_name'],
      contact_email = self.config['contact_email'],
      cdm_name = self.config['cdm_name'],
      cdm_version = self.config['cdm_version'],
      shift_date_yn = self.config['shift_date_yn'],
      max_num_shift_days = self.config['max_num_shift_days'],
      data_latency_num_days = self.config['data_latency_num_days'],
      days_between_submissions = self.config['days_between_submissions'])

    n3c_pre_cohort = self.spark.read.format('bigquery') \
      .option('table', f'{dataset_id}.n3c_pre_cohort') \
      .load()
    n3c_pre_cohort.createOrReplaceTempView('n3c_pre_cohort') 
    df = self.spark.sql(sql)
    manifest_prefix = 'MANIFEST.csv'
    if self.prefix != None:
      manifest_prefix = f'{self.prefix}/{manifest_prefix}'
    df.toPandas().to_csv(f'gs://{self.gcs_bucket}/{manifest_prefix}', index=False, sep='|', quotechar='"', quoting=csv.QUOTE_ALL, date_format='%Y-%m-%d %H:%M:%S')    
    
    print(f'Done creating MANIFEST.csv')

  def create_data_count_header(self) :
    print('Extracting data_counts/header file')

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(self.gcs_bucket)
    csv_folder = 'data_counts'
    if self.prefix != None:
      csv_folder = f'{self.prefix}/data_counts'
    blob = bucket.blob(f'{csv_folder}/header.csv')
    blob.upload_from_string('"table_name"|"row_count"\n')

    print('Done extraction data_counts/header file')

class PysparkBatchRunner:
  config : any
  gcs_bucket : str
  prefix : str
  script_file : str

  def __init__(self, config_file):
    from google.cloud import dataproc_v1
    with open(config_file, "r") as f:
      self.config = yaml.safe_load(f)
      self.gcs_bucket = self.config[gs_bucket_config]
      self.prefix = self.config[prefix_config]
      script = self.config['script_file']
      self.script_file = 'scripts/spark_extractor.py'
      if self.prefix != None:
        self.script_file = f'{self.prefix}/scripts/spark_extractor.py'
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(self.gcs_bucket)
        blob = bucket.blob(self.script_file)
        print(f'Copying {script} file to gs://{self.gcs_bucket}/{self.script_file}')
        blob.upload_from_filename(script, "text")
            
        extract_config_file = self.config['extract_config_file']
        if self.prefix != None:
          extract_config_file = f'{self.prefix}/{extract_config_file}'
        
        config_blob = bucket.blob(extract_config_file)
        print(f'Copying config file to gs://{self.gcs_bucket}/{extract_config_file}')
        config_blob.upload_from_filename(self.config['extract_config_file'], "text")
            
  def extract(self):
    cdm_tables = self.config[cdm_tables_config]
    cdm_table_list : list = cdm_tables.split(",")
    print(f'Processing {cdm_table_list}')

    # trigger pyspark batch process
    result = self.trigger_batch()

    # compose csvs for each table
    for table in cdm_table_list:
      cdm_table = table.strip()
      self.compose_csvs(cdm_table)
    self.compose_csvs('data_counts')

    cdm_table_list.append(mainfest)
    cdm_table_list.append(data_counts_folder)
    self.download_csvs(cdm_table_list)

    # cleanup files from bucket/prefix
    self.delete_csvs(data_counts_folder)
    self.delete_csvs(mainfest)
    for table in cdm_table_list:
      cdm_table = table.strip()
      self.delete_csvs(cdm_table)
        
  def trigger_batch(self):
    from google.cloud import dataproc_v1
    print('Triggering pyspark batch')
    project_id = self.config['project_id']
    service_account = self.config['service_account']
    location = self.config['region']
    subnetwork_uri = self.config['subnetwork_uri']
    batch_script = f'gs://{self.gcs_bucket}/{self.script_file}'

    extract_config_file = self.config['extract_config_file']
    if self.prefix != None:
      extract_config_file = f'{self.prefix}/{extract_config_file}'

    client = dataproc_v1.BatchControllerClient( client_options={"api_endpoint": "{}-dataproc.googleapis.com:443".format(location)})
        
    # Initialize request argument(s)
    batch = dataproc_v1.Batch()
    batch.pyspark_batch.main_python_file_uri = batch_script
    batch.pyspark_batch.jar_file_uris = ['gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.27.1.jar']
    batch.pyspark_batch.file_uris = [f'gs://{self.gcs_bucket}/{extract_config_file}']

    # Pass all the arguments you want to use in the spark job
    batch.pyspark_batch.args = ["--extract", self.config['extract_config_file']]
    environmentConfig = dataproc_v1.EnvironmentConfig()
    executionConfig = dataproc_v1.ExecutionConfig()
    executionConfig.service_account = service_account
    executionConfig.subnetwork_uri = subnetwork_uri
    environmentConfig.execution_config = executionConfig
    batch.environment_config = environmentConfig

    request = dataproc_v1.CreateBatchRequest(
      parent=f"projects/{project_id}/locations/{location}",
         batch=batch,
      )

    # Make a request to create a batch
    operation = client.create_batch(request=request)
    print('Waiting for operation to complete, this may take a while...')
    wait : int = self.config['wait_for_job_completion_in_min'] * 60 
    result = operation.result(timeout=wait)
    print(result)
    
  def delete_csvs(self, cdm_table_name) : 
    print(f'Checking if {cdm_table_name} exists in bucket {self.gcs_bucket}') 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(self.gcs_bucket)
    cdm_table_name_upper = cdm_table_name.upper()
    for page in bucket.list_blobs().pages:
      for blob in page:
        if blob.name.startswith(f'{self.prefix}') and \
          (blob.name.endswith('header.csv') == True or \
            cdm_table_name in blob.name or \
              cdm_table_name_upper in blob.name): 
          print(f'Deleting {blob.name}')
          blob.delete()
      
  def compose_csvs(self, cdm_table_name) :
    print(f'Composing csv parts for {cdm_table_name}')
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
      print ('Adding file - ' + segment.name)
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
          print("Composed from %s-%s -> %s", windex, i - 1, next_blob.name)
          windex = i
      if windex < len(segments):
        next_blob = bucket.blob("{}-tmp-{:02d}-{:02d}.csv".format(self.prefix[:-1], windex, len(segments)))
        next_blob.content_type = "text/plain"
        next_blob.compose(segments[windex:len(segments)])
        compositions.append(next_blob)
        print("Composed from %s-%s -> %s (final)", windex, len(segments), next_blob.name)
        destination.compose(compositions)

      # Delete intermediate compositions
      for composition in compositions:
        composition.delete()
    # Delete the temporary header file and all part files
    for segment in segments :
      segment.delete()
    print(f'Done composing csv parts for {cdm_table_name}')

  def download_csvs(self, cdm_table_list):
    print('Downloading csvs...')

    # check if output folder exists, if not, create one
    output_dir = self.config['output_dir']
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
      print(f'Downaloading {cdm_table_name_upper} from gcs bucket {self.gcs_bucket}') 
      
      storage_client = storage.Client()
      bucket = storage_client.get_bucket(self.gcs_bucket)

      for page in bucket.list_blobs().pages:
        for blob in page:
          if blob.name.startswith(f'{self.prefix}') and cdm_table_name_upper in blob.name and data_counts_folder not in blob.name: 
            print(f'Downlaoding {blob.name}')
            output_folder = output_dir
            if cdm_table != mainfest and cdm_table != data_counts_folder:
              output_folder = datafiles_dir
            file = f'{output_folder}/{cdm_table_name_upper}'
            content = bucket.blob(blob.name)
            content.download_to_filename(file)
            print(f'Downaloaded {output_dir}{cdm_table_name_upper}')

      print('Done downloading csvs...')
      
  
def main():
    parser = argparse.ArgumentParser(description="Utitilty to extract tables from n3c subset in to csvs")
    parser.add_argument('--config', required=False, help='specify the name of the configuration ini file to be used to trigger a spark batch, see file configuration_ini_example.txt')
    parser.add_argument('--extract', required=False, help='specify the name of the configuration ini file for extracting cdm tables, see file spark_ini_example.txt')
    
    args = parser.parse_args()

    # this part runs locally
    if args.config is not None:
      print("Running spark_extractor PysparkBatchRunner")
      config_fname = args.config
      runner = PysparkBatchRunner(config_fname)
      runner.extract()
      print("Done running spark_extractor PysparkBatchRunner")
      return
    # this part runs on cloud
    if args.extract is not None:
      print("Running spark_extractor SparkSqlBatch")
      extract_fname = args.extract
      runner = SparkSqlBatch(extract_fname)
      print("Done running spark_extractor SparkSqlBatch")
      return
    else :
      print("Supports only config command, please run following command -")
      print("e.g. --config config_ini.yaml")


if __name__ == '__main__':
    sys.exit(main())