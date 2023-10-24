
import sys
from google.cloud import storage
import argparse
import os
from pathlib import Path
import yaml
import csv
import logging

#
# This script should run on cloud
#
gs_bucket_config = 'gcs_bucket'
prefix_config = 'prefix'
cdm_tables_config = 'cdm_tables'
data_counts_folder = 'data_counts'

timestamp_format = 'yyyy-MM-dd HH:mm:ss'
mainfest = 'manifest'
additional_cdm_tables = 'additional_cdm_tables'

class spark_sql_batch:
  spark : any
  batch_config : any
  env_config : any
  gcs_bucket : str
  prefix : str
  script_file : str
  data_count_str : str

  def __init__(self, batch_config_file, env_config_file):
    from pyspark.sql import SparkSession
    with open(batch_config_file, "r") as f: 
      self.batch_config = yaml.safe_load(f)

      with open(env_config_file, "r") as ef: 
        self.env_config = yaml.safe_load(ef)
        self.spark = SparkSession.builder.appName('N3C_extract_runner').config('spark.jars', self.env_config['spark_bq_jar']).getOrCreate()
        self.gcs_bucket = self.env_config[gs_bucket_config]
        self.spark.conf.set('temporaryGcsBucket', self.gcs_bucket)
        self.prefix = self.env_config[prefix_config]

        # create MANIFEST.csv and parts for DATA_COUNTS.csv
        self.run_spark_sql_manifest()
        self.create_data_count_header()

        # extract cdm_tables from subset(persist_database_schema) dataset as csv part files
        dataset_id = self.env_config['persist_database_schema']
        cdm_tables = self.batch_config[cdm_tables_config].strip()
        if additional_cdm_tables in self.env_config:
          cdm_tables = f'{cdm_tables},{self.env_config[additional_cdm_tables].strip()}'
        cdm_table_list = cdm_tables.split(",")
        for table in cdm_table_list:
          if table in self.batch_config:       
            self.run_spark_sql(dataset_id, table.strip())
        
  def run_spark_sql(self, dataset_id, cdm_table_name):
    logging.info(f'Starting extraction of {cdm_table_name}')
    # execute the query and dump the csv parts for the cdm_table to gcs bucket
    sql = self.batch_config[cdm_table_name]
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
    rc_sql = self.batch_config['data_count_sql'].format(table_name_upper = table_name_upper, table_name = cdm_table_name)
    rc_df = self.spark.sql(rc_sql)
    rc_df.toPandas().to_csv(f'gs://{self.gcs_bucket}/{rc_foler}/{table_name_upper}.csv', header = False, index = False, sep='|', quotechar='"', quoting=csv.QUOTE_ALL)
    logging.info(f'Done extraction {cdm_table_name}')

  def run_spark_sql_manifest(self):
    logging.info(f'Extracting MANIFEST.csv file')
    dataset_id = self.env_config['results_database_schema']
    sql = self.batch_config[mainfest].format(site_abbrev = self.env_config['site_abbrev'],
      site_name = self.env_config['site_name'],
      contact_name = self.env_config['contact_name'],
      contact_email = self.env_config['contact_email'],
      cdm_name = self.env_config['cdm_name'],
      cdm_version = self.env_config['cdm_version'],
      shift_date_yn = self.env_config['shift_date_yn'],
      max_num_shift_days = self.env_config['max_num_shift_days'],
      data_latency_num_days = self.env_config['data_latency_num_days'],
      days_between_submissions = self.env_config['days_between_submissions'])

    n3c_pre_cohort = self.spark.read.format('bigquery') \
      .option('table', f'{dataset_id}.n3c_pre_cohort') \
      .load()
    n3c_pre_cohort.createOrReplaceTempView('n3c_pre_cohort') 
    df = self.spark.sql(sql)
    manifest_prefix = 'MANIFEST.csv'
    if self.prefix != None:
      manifest_prefix = f'{self.prefix}/{manifest_prefix}'
    df.toPandas().to_csv(f'gs://{self.gcs_bucket}/{manifest_prefix}', index=False, sep='|', quotechar='"', quoting=csv.QUOTE_ALL, date_format='%Y-%m-%d')    
    logging.info(f'Done creating MANIFEST.csv')

  def create_data_count_header(self) :
    logging.info('Extracting data_counts/header file')
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(self.gcs_bucket)
    csv_folder = 'data_counts'
    if self.prefix != None:
      csv_folder = f'{self.prefix}/data_counts'
    blob = bucket.blob(f'{csv_folder}/header.csv')
    blob.upload_from_string('"table_name"|"row_count"\n')
    logging.info('Done extraction data_counts/header file')

def main():
    parser = argparse.ArgumentParser(description="Utitilty to extract tables from n3c subset in to csvs")
    parser.add_argument('--batch_config', required=True, help='name of the configuration yaml file for extracting cdm tables')
    parser.add_argument('--env_config', required=True, help='name of the config ini file with environment properties')
    args = parser.parse_args()

    # this part runs on cloud
    logging.info("Running spark_extractor spark_sql_batch")
    runner = spark_sql_batch(args.batch_config, args.env_config)
    logging.info("Done running spark_extractor spark_sql_batch")

if __name__ == '__main__':
    sys.exit(main())