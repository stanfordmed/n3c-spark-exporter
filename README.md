# N3C spark extractor

![Python Logo](https://www.python.org/static/community_logos/python-logo.png "Sample inline image")

Background - The n3c extractor script https://github.com/National-COVID-Cohort-Collaborative/Phenotype_Data_Acquisition 
can successfully export smaller cdm tables into CSVs. However when the table sizes grow out of order 
e.g. measurement table having more than 2.5 billion rows can result into timing out the bigquery 
calls.

This extractor can sussesfully extract huge tables within few minutes. It uses the cdm tables gnnerated by the
N3C script https://github.com/National-COVID-Cohort-Collaborative/Phenotype_Data_Acquisition/blob/master/Exporters/PythonExporter_bigquery/db_exp.py with command - 
and dumps into CSV part files in a GCS bucket

Other references - https://github.com/National-COVID-Cohort-Collaborative/Phenotype_Data_Acquisition/wiki/Instructions-for-Sites:-OMOP-Data-Model

[The source for this project is available here][src].

To run this extractor -

1. Please run https://github.com/National-COVID-Cohort-Collaborative/Phenotype_Data_Acquisition/blob/master/Exporters/PythonExporter_bigquery/db_exp.py with command - 
    nohup python3 db_exp.py --debug --storedata --nocsv --database bigquery --config config.ini --output_dir <output-dir> --phenotype /<path-to-n3c-scripts>/Phenotype_Data_Acquisition/PhenotypeScripts/N3C_phenotype_omop_bigquery.sql --extract /<path-to-n3c-scripts>/Phenotype_Data_Acquisition/ExtractScripts/N3C_extract_omop_bigquery.sql | tee <output_log>
    --storedata option will store the tables in subset dataset
    --nocsv option ensures the cdm_table contents do not get extracted to csvs
2. Update the config.yaml file for your GCP environment i.e. set following configuration 
    project_id:  gcp project id where spark batches can be run
    service_account:  Service account email(this is same iam account you used for runiing n3c scripts)
    region:  gcp region where spark batches can be run
    subnetwork_uri: gcp subnet
    gcs_bucket: gcp bucket
    prefix: extractor where the temporary files, confiiguration files etc go and the spark batch can get the files from
    output_dir: local folder where the extractor can download CSV files to 
3. Run command -
   python3 spark_extractor.py  --config config.yaml

DO NOT CHANGE following configuration values in config.yml
    script_file:
    manifest:
    data_count_sql:
    cdm_tables: 
    person:
    observation_period:
    visit_occurrence:
    condition_occurrence:
    drug_exposure:
    device_exposure:
    procedure_occurrence:
    measurement:
    observation:
    death:
    location:
    care_site:
    provider:
    drug_era:
    condition_era: 