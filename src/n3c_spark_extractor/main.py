import argparse
from .n3c_spark_extractor import n3c_spark_extractor
import pkg_resources

def main(args=None):
  parser = argparse.ArgumentParser(description="Utitilty to extract tables from n3c subset in to csvs")
  parser.add_argument('--config', required=True, help='specify the name of the configuration yaml file to be used to trigger a spark batch, see file config_example.yaml')
    
  args = parser.parse_args()
  print("Find the debug logs in the n3c_spark_extractor.log file")
  config_fname = args.config
  runner = n3c_spark_extractor(config_fname, 
      pkg_resources.resource_filename('n3c_spark_extractor', 'spark_sql_batch.py'), 
      pkg_resources.resource_filename('n3c_spark_extractor', 'config/batch_config.yaml'))
  # runner = n3c_spark_extractor(config_fname, 
  #   'src/n3c_spark_extractor/spark_sql_batch.py', 
  #   'src/n3c_spark_extractor/config/batch_config.yaml')
  runner.extract()
  print("Done with extraction, please find the debug logs in the n3c_spark_extractor.log file")
  
if __name__ == '__main__':
  main()