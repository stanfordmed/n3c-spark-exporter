# N3C spark extractor

Background - The n3c extractor script https://github.com/National-COVID-Cohort-Collaborative/Phenotype_Data_Acquisition 
can successfully export smaller cdm tables into CSVs. However, when the table sizes grow out of order 
e.g. measurement table having more than 2.5 billion rows it can result into timing out the bigquery 
calls.

This extractor can successfully extract huge tables within few minutes. It uses the cdm tables generated by the
N3C script https://github.com/National-COVID-Cohort-Collaborative/Phenotype_Data_Acquisition/blob/master/Exporters/PythonExporter_bigquery/db_exp.py 

To run this extractor -
1. [Optional] If you plan to NOT include the controls data, create a copy of /<path-to-n3c-scripts>/Phenotype_Data_Acquisition/PhenotypeScripts/N3C_phenotype_omop_bigquery.sql with name /<path-to-n3c-scripts>/Phenotype_Data_Acquisition/PhenotypeScripts/N3C_phenotype_omop_bigquery_no_controls.sql and update -
    - insert into @resultsDatabaseSchema.n3c_cohort
        select distinct case_person_id as person_id
        from @resultsDatabaseSchema.n3c_control_map

        union distinct select distinct control_person_id
        from @resultsDatabaseSchema.n3c_control_map
        where control_person_id is not null;
        to be -
        insert into @resultsDatabaseSchema.n3c_cohort
        select distinct case_person_id as person_id
        from @resultsDatabaseSchema.n3c_control_map;
        
2.  Run https://github.com/National-COVID-Cohort-Collaborative/Phenotype_Data_Acquisition/blob/master/Exporters/PythonExporter_bigquery/db_exp.py with command - 
    - [Optional] If you plan to NOT include the controls data, run command - 
        - nohup python3 db_exp.py --debug --storedata --nocsv --database bigquery --config config.ini --output_dir <output-dir> --phenotype /<path-to-n3c-scripts>/Phenotype_Data_Acquisition/PhenotypeScripts/N3C_phenotype_omop_bigquery_no_controls.sql --extract /<path-to-n3c-scripts>/Phenotype_Data_Acquisition/ExtractScripts/N3C_extract_omop_bigquery.sql | tee <output_log>
    - Otherwise run - 
        - nohup python3 db_exp.py --debug --storedata --nocsv --database bigquery --config config.ini --output_dir <output-dir> --phenotype /<path-to-n3c-scripts>/Phenotype_Data_Acquisition/PhenotypeScripts/N3C_phenotype_omop_bigquery.sql --extract /<path-to-n3c-scripts>/Phenotype_Data_Acquisition/ExtractScripts/N3C_extract_omop_bigquery.sql | tee <output_log>
            - --storedata option will store the tables in subset dataset
            - --nocsv option ensures the cdm_table contents do not get extracted to csvs
2. Update the config.yaml file for your GCP environment i.e. set following configuration 
    - project_id:  gcp project id where spark batches can be run
    - service_account:  Service account email (this is same iam account you used for running n3c scripts)
    - region:  gcp region where spark batches can be run
    - subnetwork_uri: gcp subnet
    - gcs_bucket: gcp bucket
    - prefix: extractor where the temporary files, configuration files etc go and the spark batch can get the files from
    - output_dir: local folder where the extractor can download CSV files to. NOTE: If executing on Windows host, use Windows path syntax (e.g., C:\output_dir). Do not use Linux syntax (/c/output_dir). 
    - additional_cdm_tables: OPTIONAL if additional_cdm_tables is not supplied, it will extract only - [person,observation_period,visit_occurrence,condition_occurrence,drug_exposure,device_exposure,procedure_occurrence,measurement,observation,death,location,care_site,provider,drug_era,condition_era], for any additonal cdm tables supply the remaining tables list and the sql for those tables (refer to config_example.yaml file). Only add the table name (e.g. note_nlp) to the select clause in config.yaml file. Do not include project_id.dataset to the SQL statements there. No spaces are allowed between table names in the addition_cdm_tables field.
3. Clone the repository -
    - https://github.com/stanfordmed/n3c-spark-exporter
4. Build the module - 
    - python -m build
5. Install the package -
    -  pip install dist/n3c_spark_extractor-1.0.4.tar.gz
6. Run -
    - n3c_spark_extractor --config config.yaml 
7. Zip the CSVs and sftp as https://github.com/National-COVID-Cohort-Collaborative/Phenotype_Data_Acquisition/wiki/Instructions-for-Sites:-OMOP-Data-Model 

NOTES - 
    -  NOTE: If executing on Windows host, use Windows path syntax (e.g., C:\output_dir). Do not use Linux syntax (/c/output_dir). 
    - The timeout_in_min is set to 60 mins, the pyspark batch generally takes 15-20 minutes to complete. In future if it takes more than 60 minutes, config timeout_in_min can be set to a higher value. 
    - If you happen to kill the n3c_spark_extractor process and if the spark job was already triggered, wait until it finishes before you run n3c_spark_extractor again
DO NOT CHANGE configuration values in batch_config.yml

Version history -
    - 1.0.0 - Initial version
    - 1.0.1 - Fix for passing bigquery-jar file location in yaml file
    - 1.0.2 - Added a configuration delete_merged_csvs_from_bucket to disable deletion merged CSV files from GCP bucket
    - 1.0.3 - Removed timestamps in MANIFEST.csv and updated config_example.yaml
    - 1.0.4 - Changed column names in MANIFEST.csv to be usppercase
