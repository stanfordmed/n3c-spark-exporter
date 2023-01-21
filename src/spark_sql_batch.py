
import sys
from google.cloud import storage
import argparse
import os
from pathlib import Path
import yaml
import csv
#
# This script should run on cloud
#
spark_bq_jar = 'gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.27.1.jar'
gs_bucket_config = 'gcs_bucket'
prefix_config = 'prefix'
cdm_tables_config = 'cdm_tables'
data_counts_folder = 'data_counts'

timestamp_format = 'yyyy-MM-dd HH:mm:ss'
mainfest = 'manifest'

class spark_sql_batch:
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
          self.run_spark_sql(dataset_id, table.strip())
        
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

def main():
    parser = argparse.ArgumentParser(description="Utitilty to extract tables from n3c subset in to csvs")
    parser.add_argument('--extract', required=False, help='specify the name of the configuration ini file for extracting cdm tables, see file spark_ini_example.txt')
    args = parser.parse_args()

    # this part runs on cloud
    if args.extract is not None:
      print("Running spark_extractor SparkSqlBatch")
      extract_fname = args.extract
      runner = spark_sql_batch(extract_fname)
      print("Done running spark_extractor SparkSqlBatch")
      return

if __name__ == '__main__':
    sys.exit(main())