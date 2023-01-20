import argparse
from .n3c_spark_extractor import *

def main(args=None):
  parser = argparse.ArgumentParser(description="Utitilty to extract tables from n3c subset in to csvs")
  parser.add_argument('--config', required=False, help='specify the name of the configuration yaml file to be used to trigger a spark batch, see file config_example.yaml')
    
  args = parser.parse_args()
  if args.config is not None:
    print("Running n3c_spark_extractor")
    config_fname = args.config
    runner = n3c_spark_extractor(config_fname)
    runner.extract()
    print("Done running n3c_spark_extractor")
    return
  else :
    print("Supports only config command, please run following command -")
    print("e.g. --config config_ini.yaml")

if __name__ == '__main__':
  main()