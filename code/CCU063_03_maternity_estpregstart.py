# Databricks notebook source
# MAGIC %md
# MAGIC #Pregnancy start date estimation

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### MSDS Pregnancy start date estimation options:
# MAGIC
# MAGIC There is one field, eddagreed, derived from other fields. Based on clinical advice we are prioritising as follows
# MAGIC
# MAGIC - 1st option: Eddagreed, method 02 (Variable: eddagreed, demo table) minus 280 days
# MAGIC - 2nd option: Eddagreed, method 03 (Variable: eddagreed, demo table) minus 280 days
# MAGIC - 3rd option: Procedure Date [Dating Ultrasound Scan, demo table] (Variable: proceduredatedatingultrasound) minus Gestational age [dating ultrasound] (Variable: gestagedatultradate , demo table)
# MAGIC - 4th option: Eddagreed, method 01 (Variable: eddagreed, demo table) minus 280 days
# MAGIC - 5th option: Last Menstrual Period date (Variable: lastmenstrualperioddate, demo table)
# MAGIC - 6th option: Eddagreed, method 04 (Variable: eddagreed, demo table) minus 280 days
# MAGIC - 7th option: Appointment date [formal antenatal booking] (Variable: antenatalappdate, demo table) minus gestational age at booking (variable: gestagebooking , demo table)
# MAGIC
# MAGIC Not currently developed
# MAGIC - Final option: Date of birth baby (day- which unavailbe so is imputted as first- month and year combined) minus 280. 
# MAGIC
# MAGIC Where multiple records per source, we try to take the earliest activity however this is not always possible. Most will fall within a week of one another however.
# MAGIC
# MAGIC **Eddagreed source**  
# MAGIC 01 - Last Menstrual Period (LMP) date as stated by the mother  
# MAGIC 02 - Last Menstrual Period (LMP) date confirmed by Ultrasound Scan In Pregnancy  
# MAGIC 03 - Ultrasound Scan In Pregnancy dating measurements  
# MAGIC 04 - Clinical assessment
# MAGIC
# MAGIC ### HES APC Mat Pregnancy start date estimation options:
# MAGIC Could use code from CCU018_02-D03-cohort Section 5 to write code to estimate pregnancy start date from HES APC Mat - we could adapt this to add anyone who doesn't have a pregnancy start date from MSDS. Preferred over option 3, but after option 2.
# MAGIC
# MAGIC
# MAGIC
# MAGIC Note: GESTAT is the length of gestation in HES APC Maternity

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
#msds_labour = extract_batch_from_archive(parameters_df_datasets, 'msds_labour')
msds_demo = extract_batch_from_archive(parameters_df_datasets, 'msds_demo')
#msds_diag = extract_batch_from_archive(parameters_df_datasets, 'msds_diag')
#msds_care = extract_batch_from_archive(parameters_df_datasets, 'msds_care')
#msds_assessments = extract_batch_from_archive(parameters_df_datasets, 'msds_assessments')
#msds_findings = extract_batch_from_archive(parameters_df_datasets, 'msds_findings')
#msds_hospital = extract_batch_from_archive(parameters_df_datasets, 'msds_hospital')
#msds_careplan = extract_batch_from_archive(parameters_df_datasets, 'msds_careplan')

# reading in baby tables
#msds_baby_demo = extract_batch_from_archive(parameters_df_datasets, 'msds_baby_demo')
#msds_baby_activities = extract_batch_from_archive(parameters_df_datasets, 'msds_baby_activities')
#msds_baby_diag = extract_batch_from_archive(parameters_df_datasets, 'msds_baby_diag')


# COMMAND ----------

#msds_labour = msds_labour.select('person_id_mother_deid','uniqpregid').dropDuplicates().withColumn('labour', f.lit(1))
msds_demo = msds_demo.select('person_id_mother_deid','uniqpregid').dropDuplicates().withColumn('demo', f.lit(1))
#msds_diag = msds_diag.select('person_id_mother_deid','uniqpregid').dropDuplicates().withColumn('diag', f.lit(1))
#msds_care = msds_care.select('person_id_mother_deid','uniqpregid').dropDuplicates().withColumn('care', f.lit(1))
#msds_assessments = msds_assessments.select('person_id_mother_deid','uniqpregid').dropDuplicates().withColumn('assessments', f.lit(1))
#msds_findings = msds_findings.select('person_id_mother_deid','uniqpregid').dropDuplicates().withColumn('findings', f.lit(1))
#msds_hospital = msds_hospital.select('person_id_mother_deid','uniqpregid').dropDuplicates().withColumn('hospital', f.lit(1))
#msds_careplan = msds_careplan.select('person_id_mother_deid','uniqpregid').dropDuplicates().withColumn('careplan', f.lit(1))


#msds_baby_demo = msds_baby_demo.select('person_id_mother_deid','uniqpregid', 'person_id_baby_deid').dropDuplicates().withColumn('baby_demo', f.lit(1))
#msds_baby_activities = msds_baby_activities.select('person_id_mother_deid','uniqpregid', 'person_id_baby_deid').dropDuplicates().withColumn('baby_activities', f.lit(1))
#msds_baby_diag = msds_baby_diag.select('person_id_mother_deid','uniqpregid', 'person_id_baby_deid').dropDuplicates().withColumn('baby_diag', f.lit(1))



# COMMAND ----------

table_keys

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # 6 Preg start date

# COMMAND ----------

msds_demo_preg_start = extract_batch_from_archive(parameters_df_datasets, "msds_demo")

id_fields = ["person_id_mother_deid", "uniqpregid"]

msds_demo_preg_start = ( msds_demo_preg_start
                        .select(
    "person_id_mother_deid",
    "uniqpregid",
    "eddagreed",
    "eddmethodagreed",
    "proceduredatedatingultrasound",
    "gestagedatultradate",
    "lastmenstrualperioddate",
    "antenatalappdate",
    "gestagebooking",
)
                        .where(f.col("person_id_mother_deid").isNotNull())
                        .where(f.col("uniqpregid").isNotNull())
)


# COMMAND ----------

# DBTITLE 1,Prep'ing component fields
# - 1st option: Estimated date of delivery (Variable: eddagreed, demo table) minus 280 days
priority_eddagreed = (msds_demo_preg_start
             .select( *id_fields , "eddagreed", "eddmethodagreed", "proceduredatedatingultrasound", "antenatalappdate" )
             .where(f.col("eddagreed").isNotNull())
             .where(f.col("eddmethodagreed").isNotNull())
             .where(f.col("eddmethodagreed") != "NA")
             .where(f.col("eddmethodagreed") != "99")
             .withColumn("est_preg_start", f.date_sub("eddagreed", 280))
             .withColumn("priority", f.when( f.col("eddmethodagreed") == "03" , f.lit(1))
                         .when( f.col("eddmethodagreed") == "02" , f.lit(2))
                         .when( f.col("eddmethodagreed") == "01" , f.lit(4))
                         .when( f.col("eddmethodagreed") == "04" , f.lit(6))
                         .otherwise(f.lit(99)) )
             .withColumn("start_source",  f.concat_ws("_" , f.lit("eddagreed"), f.col("eddmethodagreed") ) )
             .withColumn("source_date", f.when( f.col("priority").isin([1,2]) , f.col("proceduredatedatingultrasound"))
                         .when( f.col("priority").isin([4,6]) , f.col("antenatalappdate"))
                         )
             .drop("eddagreed", "eddmethodagreed" ,  "proceduredatedatingultrasound", "antenatalappdate")
             .dropDuplicates()
)


# - 3rd option: Procedure Date [Dating Ultrasound Scan, demo table] (Variable: proceduredatedatingultrasound) minus Gestational age [dating ultrasound] (Variable: gestagedatultradate , demo table)
priority3 = (msds_demo_preg_start
             .select(*id_fields , "proceduredatedatingultrasound" , "gestagedatultradate" )
             .where(f.col("proceduredatedatingultrasound").isNotNull())
             .where(f.col("gestagedatultradate").isNotNull())
             .withColumn("gestagedatultradate" , f.col("gestagedatultradate").cast("int"))
             .withColumn("est_preg_start", f.date_sub("proceduredatedatingultrasound", "gestagedatultradate"))
             .withColumn("priority", f.lit(3))
             .withColumn("start_source", f.lit("proceduredatedatingultrasound") )
             .withColumn("source_date" , f.col("proceduredatedatingultrasound"))
             .drop("proceduredatedatingultrasound" , "gestagedatultradate" )
             .dropDuplicates()
)


#- 5th option: Last Menstrual Period date (Variable: lastmenstrualperioddate, demo table)
priority5 = (msds_demo_preg_start
             .select(*id_fields , "lastmenstrualperioddate", "antenatalappdate")
             .where(f.col("lastmenstrualperioddate").isNotNull())
             .withColumnRenamed( "lastmenstrualperioddate" , "est_preg_start")
             .withColumn("priority", f.lit(5))
             .withColumn("start_source", f.lit("lastmenstrualperioddate") )
             .withColumn("source_date" , f.col("antenatalappdate"))
             .drop( "antenatalappdate")
             .dropDuplicates()
)

# - 7th option: Appointment date [formal antenatal booking] (Variable: antenatalappdate, demo table) minus gestational age at booking (variable: gestagebooking , demo table)
priority7 = (msds_demo_preg_start
             .select(*id_fields , "antenatalappdate" , "gestagebooking" )
             .where(f.col("antenatalappdate").isNotNull())
             .where(f.col("gestagebooking").isNotNull())
             .sort( *id_fields , "antenatalappdate" )
             .withColumn("gestagebooking" , f.col("gestagebooking").cast("int"))
             .withColumn("est_preg_start", f.date_sub("antenatalappdate", "gestagebooking"))
             .withColumn("priority", f.lit(7))
             .withColumn("start_source", f.lit("antenatalappdate") )
             .withColumn("source_date" , f.col("antenatalappdate"))
             .drop("antenatalappdate" , "gestagebooking" )
             .dropDuplicates()
)

# - 5th option: Date of birth baby (day, month and year combined) minus 280

# COMMAND ----------

if checks_on:
    primary_key = id_fields
    df = priority_eddagreed

    not_duplicate_records = df.groupBy(primary_key).count().where('count = 1').drop('count')
    duplicate_records = df.join(not_duplicate_records, on=primary_key, how='left_anti')

    display(duplicate_records, limit=100)
    #display(not_duplicate_records, limit=100)

# COMMAND ----------

# DBTITLE 1,selecting priority start date
from pyspark.sql import Window
w = Window.partitionBy("uniqpregid")

maternity_start = ( priority_eddagreed
                   .union(priority3)
                   .union(priority5)
                   .union(priority7)
                   .sort("person_id_mother_deid", "uniqpregid", "priority", "source_date")
                   .withColumn("record_to_keep", f.min("priority").over(w))
                   .dropDuplicates()
                   .filter(f.col("priority") == f.col("record_to_keep") )
                   .withColumn("earliest_date", f.min("source_date").over(w))
                   .dropDuplicates()
                   .withColumn("if_keeping_earliest", f.col("source_date") == f.col("earliest_date") )
#                   .where(f.col("if_keeping_earliest") == True)
                   .sort("person_id_mother_deid", "uniqpregid", "priority", "source_date")
                   .dropDuplicates(["person_id_mother_deid", "uniqpregid"])
)



# COMMAND ----------

if checks_on:
    maternity_start.printSchema()
    display(maternity_start.sort("est_preg_start", "priority"), limit=100)
    tab( maternity_start, "priority")
    count_var( maternity_start, "uniqpregid")
    tab( maternity_start.withColumn('missing_date' , f.col("source_date").isNull()) , 'missing_date' , 'priority' )

# COMMAND ----------

# DBTITLE 1,save
outName = f'{proj}_msds_preg_start'

# save
maternity_start.write.mode('overwrite').option("overwriteSchema", "true").saveAsTable(f'{dbc}.{outName}')

# COMMAND ----------

# DBTITLE 1,reading back
maternity_start = spark.table(f'{dbc}.{proj}_msds_preg_start')

# COMMAND ----------

# DBTITLE 1,checks
if checks_on:
    display(maternity_start.sort("est_preg_start", "priority"), limit=100)
    tab( maternity_start, "priority")
count_var( maternity_start, "uniqpregid")

# COMMAND ----------

# DBTITLE 1,coverage over time
preg_start_coverage = (maternity_start
                  .withColumn('est_preg_start_month', f.date_format("est_preg_start", 'yyyy-MM'))
                  .drop('est_preg_start')
                  .dropDuplicates()
                  .groupBy('est_preg_start_month', 'start_source')
                    .agg(f.count(f.col('uniqpregid')).alias('cohort_recs'))
#                    .withColumn('cohort_recs_month', f.sum(f.col('cohort_recs')).over(Window.partitionBy('est_preg_start_month')))     
#                    .withColumn('msds_matched_prop', f.col('cohort_recs')/f.col('cohort_recs_month'))
                .sort(['est_preg_start_month', 'start_source'])
)

# if checks_on:
display(preg_start_coverage)