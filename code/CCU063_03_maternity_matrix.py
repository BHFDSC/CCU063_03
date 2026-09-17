# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC #0 Parameters

# COMMAND ----------

spark.catalog.clearCache()

# COMMAND ----------

# MAGIC %run "/Shared/SHDS/common/functions"

# COMMAND ----------

# function to extract the batch corresponding to the pre-defined archived_on date from the archive for the specified dataset
from pyspark.sql import DataFrame
def extract_batch_from_archive(_df_datasets: DataFrame, _dataset: str):
  
  # get row from df_archive_tables corresponding to the specified dataset
  _row = _df_datasets[_df_datasets['dataset'] == _dataset]
  
  # check one row only
  assert _row.shape[0] != 0, f"dataset = {_dataset} not found in _df_datasets (datasets = {_df_datasets['dataset'].tolist()})"
  assert _row.shape[0] == 1, f"dataset = {_dataset} has >1 row in _df_datasets"
  
  # create path and extract archived on
  _row = _row.iloc[0]
  _path = _row['database'] + '.' + _row['table']  
  _archived_on = _row['archived_on']  
  print(_path + ' (archived_on = ' + _archived_on + ')')
  
  # check path exists # commented out for runtime
#   _tmp_exists = spark.sql(f"SHOW TABLES FROM {_row['database']}")\
#     .where(f.col('tableName') == _row['table'])\
#     .count()
#   assert _tmp_exists == 1, f"path = {_path} not found"

  # extract batch
  _tmp = spark.table(_path)\
    .where(f.col('archived_on') == _archived_on)  
  
  # check number of records returned
  _tmp_records = _tmp.count()
  print(f'  {_tmp_records:,} records')
  assert _tmp_records > 0, f"number of records == 0"

  # return dataframe
  return _tmp

# COMMAND ----------

import pyspark.sql.functions as f
import pyspark.sql.types as t
import pandas as pd
import re
from pyspark.sql import Window
import seaborn as sns
import matplotlib as mpl

# COMMAND ----------

# MAGIC %md
# MAGIC # 1 Data

# COMMAND ----------

# -----------------------------------------------------------------------------
# Databases
# -----------------------------------------------------------------------------
db = ''
dbc_old = f'{db}_collab'
dbc = ''

proj = 'ccu063_03'
checks_on = False

# dataframe of required datasets

# MSDS
# Using newer data (Feb 2024 batch), and a newer version (v2 which should include data loss fixes)
tmp_archived_on = '2024-12-02'
data = [
    ['msds_labour', dbc_old, f'msds_v2_labour_activities_all_years_archive',      tmp_archived_on]
  , ['msds_demo',  dbc_old, f'msds_v2_demographics_booking_and_pregnancy_all_years_archive',       tmp_archived_on]
  , ['msds_diag',  dbc_old, f'msds_v2_diagnoses_and_history_all_years_archive',       tmp_archived_on]
  , ['msds_care',  dbc_old, f'msds_v2_care_activities_all_years_archive',       tmp_archived_on]
  , ['msds_assessments',  dbc_old, f'msds_v2_coded_scored_assessments_all_years_archive',       tmp_archived_on]
  , ['msds_findings',  dbc_old, f'msds_v2_findings_and_observations_all_years_archive',       tmp_archived_on]
  , ['msds_hospital',  dbc_old, f'msds_v2_hospital_provider_spell_all_years_archive',       tmp_archived_on]
  , ['msds_careplan',  dbc_old, f'msds_v2_maternity_care_plan_all_years_archive',       tmp_archived_on]

  , ['msds_baby_activities',  dbc_old, f'msds_v2_baby_activities_all_years_archive',       tmp_archived_on]
  , ['msds_baby_demo',  dbc_old, f'msds_v2_baby_demographics_all_years_archive',       tmp_archived_on]
  , ['msds_baby_diag',  dbc_old, f'msds_v2_baby_diagnoses_all_years_archive',       tmp_archived_on]
]
parameters_df_datasets = pd.DataFrame(data, columns = ['dataset', 'database', 'table', 'archived_on'])

# CCU018_02 cohort - TO DELETE - this is simply a reference point to how other projects have curated their mat linkage
# ccu018_02_cohort = spark.table(f'{dbc}.{proj}_out_cohort')

# COMMAND ----------

# DBTITLE 1,table keys
#These keys define the person_id, event_date and primary_key columns for each table, to then be used for metrics calculations
# should you wish to add a new table please insert the table name given below instead of its archive name

table_keys = {
  
  'msds_v2_anonymous':{
        'person_id':   None,
        'event_date':  'ClinInterDate',
        'primary_key': None},
  'msds_v2_baby_activities':{
        'person_id':   'Person_ID_Baby_DEID',
        'event_date':  'ClinInterDateBaby',
        'primary_key': None},
  'msds_v2_baby_demographics':{
        'person_id':   'Person_ID_Baby_DEID',
        'event_date':  'NeonatalTransferStartDate',
        'primary_key': None},
  'msds_v2_baby_diagnoses':{
        'person_id':   'Person_ID_Baby_DEID',
        'event_date':  'DiagnosisDate',
        'primary_key': None},
  'msds_v2_care_activities':{
        'person_id':   'Person_ID_Mother_DEID',
        'event_date':  'CContactDate',
        'primary_key': 'pseudo_uniquecareactivityid_mother'},
  'msds_v2_coded_scored_assessments':{
        'person_id':   'Person_ID_Mother_DEID',
        'event_date':  'CompDate',
        'primary_key': None},
  'msds_v2_demographics_booking_and_pregnancy':{
        'person_id':   'Person_ID_Mother_DEID',
        'event_date':  'AntenatalAppDate',
        'primary_key': None},
  'msds_v2_diagnoses_and_history':{
        'person_id':   'Person_ID_Mother_DEID',
        'event_date':  'DiagnosisDate',
        'primary_key': None},
  'msds_v2_findings_and_observations':{
        'person_id':   'Person_ID_Mother_DEID',
        'event_date':  'FindingDate',
        'primary_key': None},
  'msds_v2_hospital_provider_spell':{
        'person_id':   'Person_ID_Mother_DEID',
        'event_date':  'StartDateHospProvSpell',
        'primary_key': 'pseudo_uniquehospprovspellnum'},
  'msds_v2_labour_activities':{
        'person_id':   'Person_ID_Mother_DEID',
        'event_date':  'ClinInterDateMother',
        'primary_key': 'pseudo_uniquelabourdeliveryid'},
  'msds_v2_maternity_care_plan':{
        'person_id':   'Person_ID_Mother_DEID',
        'event_date':  'CarePlanDate',
        'primary_key':  None}


}



def get_asset_keys(table_name: str) -> dict:
  
  if table_name in table_keys:
    return table_keys[table_name]
  
  else:
    print(f'Table "{table_name}" not found!')

# COMMAND ----------



# COMMAND ----------

# reading in mother tables
msds_labour = extract_batch_from_archive(parameters_df_datasets, 'msds_labour')
msds_demo = extract_batch_from_archive(parameters_df_datasets, 'msds_demo')
msds_diag = extract_batch_from_archive(parameters_df_datasets, 'msds_diag')
msds_care = extract_batch_from_archive(parameters_df_datasets, 'msds_care')
msds_assessments = extract_batch_from_archive(parameters_df_datasets, 'msds_assessments')
msds_findings = extract_batch_from_archive(parameters_df_datasets, 'msds_findings')
msds_hospital = extract_batch_from_archive(parameters_df_datasets, 'msds_hospital')
msds_careplan = extract_batch_from_archive(parameters_df_datasets, 'msds_careplan')

# reading in baby tables
msds_baby_demo = extract_batch_from_archive(parameters_df_datasets, 'msds_baby_demo')
msds_baby_activities = extract_batch_from_archive(parameters_df_datasets, 'msds_baby_activities')
msds_baby_diag = extract_batch_from_archive(parameters_df_datasets, 'msds_baby_diag')


# COMMAND ----------

msds_labour = msds_labour.select('person_id_mother_deid','uniqpregid').dropDuplicates().withColumn('labour', f.lit(1))
msds_demo = msds_demo.select('person_id_mother_deid','uniqpregid').dropDuplicates().withColumn('demo', f.lit(1))
msds_diag = msds_diag.select('person_id_mother_deid','uniqpregid').dropDuplicates().withColumn('diag', f.lit(1))
msds_care = msds_care.select('person_id_mother_deid','uniqpregid').dropDuplicates().withColumn('care', f.lit(1))
msds_assessments = msds_assessments.select('person_id_mother_deid','uniqpregid').dropDuplicates().withColumn('assessments', f.lit(1))
msds_findings = msds_findings.select('person_id_mother_deid','uniqpregid').dropDuplicates().withColumn('findings', f.lit(1))
msds_hospital = msds_hospital.select('person_id_mother_deid','uniqpregid').dropDuplicates().withColumn('hospital', f.lit(1))
msds_careplan = msds_careplan.select('person_id_mother_deid','uniqpregid').dropDuplicates().withColumn('careplan', f.lit(1))


msds_baby_demo = msds_baby_demo.select('person_id_mother_deid','uniqpregid', 'person_id_baby_deid').dropDuplicates().withColumn('baby_demo', f.lit(1))
msds_baby_activities = msds_baby_activities.select('person_id_mother_deid','uniqpregid', 'person_id_baby_deid').dropDuplicates().withColumn('baby_activities', f.lit(1))
msds_baby_diag = msds_baby_diag.select('person_id_mother_deid','uniqpregid', 'person_id_baby_deid').dropDuplicates().withColumn('baby_diag', f.lit(1))



# COMMAND ----------

table_keys

# COMMAND ----------

# MAGIC %md
# MAGIC # 3 Creating Matrix

# COMMAND ----------

msds_matrix = (msds_labour.drop('datestamp' , 'datestamp_source')
               .join(msds_demo.drop('datestamp', 'datestamp_source'), on= ['person_id_mother_deid', 'uniqpregid'], how='full')
               .join(msds_diag.drop('datestamp', 'datestamp_source'), on= ['person_id_mother_deid', 'uniqpregid'], how='full')
               .join(msds_care.drop('datestamp', 'datestamp_source'), on= ['person_id_mother_deid', 'uniqpregid'], how='full')
               .join(msds_assessments.drop('datestamp', 'datestamp_source'), on= ['person_id_mother_deid', 'uniqpregid'], how='full')
               .join(msds_findings.drop('datestamp', 'datestamp_source'), on= ['person_id_mother_deid', 'uniqpregid'], how='full')
               .join(msds_hospital.drop('datestamp', 'datestamp_source'), on= ['person_id_mother_deid', 'uniqpregid'], how='full')
               .join(msds_careplan.drop('datestamp', 'datestamp_source'), on= ['person_id_mother_deid', 'uniqpregid'], how='full')
    
)

msds_matrix.cache()
display(msds_matrix.sort("person_id_mother_deid").limit(10))

# COMMAND ----------

# DBTITLE 1,baby subtable flag matrix
msds_baby_matrix = (
    msds_baby_demo.drop('datestamp', 'datestamp_source')
    .join(msds_baby_activities.drop('datestamp', 'datestamp_source'), on= ['person_id_mother_deid', 'uniqpregid'], how='full')
    .join(msds_baby_diag.drop('datestamp', 'datestamp_source'), on= ['person_id_mother_deid', 'uniqpregid'], how='full')
               
)

msds_baby_matrix.cache()

# COMMAND ----------

# DBTITLE 1,Combined mother baby subtables
mother_baby_linkage = (msds_matrix
                       .withColumn('mother_table', f.lit(1))
                       .join(msds_baby_matrix
                             .withColumn('baby_table', f.lit(1))
                             , on= ['person_id_mother_deid', 'uniqpregid'], how='full')
                       
)

mother_baby_linkage.cache()

if checks_on:
      display(mother_baby_linkage.sort("person_id_mother_deid").limit(10))
      #tab(mother_baby_linkage,  'mother_table',  'baby_table')
      #tab(mother_baby_linkage,  'baby_table')
      

# COMMAND ----------

display(mother_baby_linkage.sort("person_id_mother_deid").limit(10))

# COMMAND ----------

outName = f'{proj}_mother_baby_linkage'

# save
mother_baby_linkage.write.mode('overwrite').option("overwriteSchema", "true").saveAsTable(f'{dbc}.{outName}')

# COMMAND ----------

spark.catalog.clearCache()

# COMMAND ----------

# MAGIC %md
# MAGIC # 4 Read back

# COMMAND ----------

# DBTITLE 1,Reading back to save compute in latter half notebook
# maternity_flags = spark.table(f'{dbc}.{proj}_mother_baby_linkage')
maternity_dates = spark.table(f'{dbc}.{proj}_msds_record_daterange')

# COMMAND ----------

if checks_on:
    display(maternity_flags.limit(1))
    display(maternity_dates.limit(1))
    count_var( msds_matrix, 'uniqpregid')
    count_var( msds_matrix, 'person_id_mother_deid')
    # record the below to upset plot if you want to see all the combinations
    tab(msds_matrix, 'demo')
    tab(msds_matrix, 'labour')
    tab(msds_matrix, 'diag')
    tab(msds_matrix, 'care')
    tab(msds_matrix, 'assessments')
    tab(msds_matrix, 'findings')
    tab(msds_matrix, 'hospital')
    tab(msds_matrix, 'careplan')
    tab(msds_baby_matrix, 'baby_demo')
    tab(msds_baby_matrix, 'baby_diag')
    tab(msds_baby_matrix, 'baby_activities')

