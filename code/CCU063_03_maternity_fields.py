# Databricks notebook source
# MAGIC %md
# MAGIC ## Creating a linkage matrix for maternity records
# MAGIC
# MAGIC **Purpose:** The cohort in this project is largely based on mothers and their records shortly prior to pregnancy. Maternity records will be needed to help define this cohort, and capture key information relevant to the study. The maternity records will come from two possible sources - msds and hes_mat. This notebook establishes the available mothers, links the various datasets and allows early filtering to the date specific criteria of the project. 
# MAGIC
# MAGIC NB the goal is to formalise which mothers and pregnancies should be captured by this study. Therefore, only information relevant to this will appear in this notebook, but other fields might be brought in elsewhere in the pipeline. Likewise, there may be other reasons to exclude some mothers/pregnancies based on other criteria - so cohort established at the end of this notebook is only provisional.
# MAGIC
# MAGIC **Author(s):** Majel McGranahan and Lars Murdock

# COMMAND ----------

# MAGIC %md
# MAGIC ### Instructions for notebook dev
# MAGIC
# MAGIC There are some key steps needed to achieve the goals stated above so this is the checklist. I suggest we apply them to msds to begin with then bring in hes_mat after
# MAGIC
# MAGIC ### MSDS curation
# MAGIC
# MAGIC - read in the minimal msds_subtables for all msds
# MAGIC - make sure a datestamp field is read in with
# MAGIC - ensure the fields are consistent and union (append) all the tables into one long list
# MAGIC - partition on motherid and uniqpregid then create two fields - one for oldest appearing record, one for latest.
# MAGIC - minimise to one row per patient, query the datediff to check the quality (i.e. are any pregs appears to span multiple years)
# MAGIC
# MAGIC - filter out any pregnancies with quality issues, or those which place the pregnancy outside the bounds of the study period
# MAGIC - the remaining pregs have passed the first eligibility checks, and form the skeleton for further checks
# MAGIC
# MAGIC - establish (in writing not code) which fields in msds can be used to calculate preg_start_date, decide the estimation/any imputation, and how these would be prioritised
# MAGIC
# MAGIC - read in these fields, and derivation, and join to the skeleton table
# MAGIC - filter for missingness
# MAGIC - conduct quality checks to eliminate implausible start dates/preg lengths
# MAGIC - apply prioritisation such that there is only one estimated start date per pregnancy
# MAGIC - query data for further dist checks
# MAGIC - filter out pregs that now do not fall within the study period based on the estimatepreg startdate
# MAGIC
# MAGIC The remaining will be your more developed maternity cohort.
# MAGIC
# MAGIC You might want to create a flag for these, and any msds subtables, and finally any indication of delivery/baby, and delivery date. For the time being you'll also want to keep the source of the est_preg_start_date
# MAGIC
# MAGIC Following this a similar process for hes_mat can be done and then joined.
# MAGIC
# MAGIC ### NB about hes_mat
# MAGIC - hes_mat might need to be joined with hes_apc on epikey to establish datestamp
# MAGIC - hes_mat will only feature deliveries that made it to term
# MAGIC - hes_mat gest field is only 75% complete, and expressed in weeks not days
# MAGIC
# MAGIC ### NB about joining hes_mat onto your msds derived cohort
# MAGIC - you'll only be able to join onto the msds pregs that made it to term
# MAGIC - some will only feature in hes_mat so can't be joined
# MAGIC - when joining hes_mat doesn't have a uniqpreg field so joining will need to take place on mother_id and month of delivery +/- 1, with some curation required
# MAGIC - once joined you need to decide where, for records with msds/hesmat completing preg start dates which to prioritise
# MAGIC
# MAGIC
# MAGIC
# MAGIC

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
checks_on = True

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
msds_labour = extract_batch_from_archive(parameters_df_datasets, 'msds_labour').cache()
msds_demo = extract_batch_from_archive(parameters_df_datasets, 'msds_demo').cache()
#msds_diag = extract_batch_from_archive(parameters_df_datasets, 'msds_diag').cache()
msds_care = extract_batch_from_archive(parameters_df_datasets, 'msds_care').cache()
msds_assessments = extract_batch_from_archive(parameters_df_datasets, 'msds_assessments').cache()
#msds_findings = extract_batch_from_archive(parameters_df_datasets, 'msds_findings').cache()
msds_hospital = extract_batch_from_archive(parameters_df_datasets, 'msds_hospital').cache()
msds_careplan = extract_batch_from_archive(parameters_df_datasets, 'msds_careplan').cache()

# reading in baby tables
msds_baby_demo = extract_batch_from_archive(parameters_df_datasets, 'msds_baby_demo').cache()
msds_baby_activities = extract_batch_from_archive(parameters_df_datasets, 'msds_baby_activities').cache()
msds_baby_diag = extract_batch_from_archive(parameters_df_datasets, 'msds_baby_diag').cache()


# COMMAND ----------

table_keys

# COMMAND ----------

# MAGIC %md
# MAGIC # 2 Shortlisting by earliest-latest record

# COMMAND ----------

msds_labour = msds_labour.select('person_id_mother_deid','uniqpregid', f.col('ClinInterDateMother').alias("datestamp")).dropDuplicates().withColumn('labour', f.lit(1)).withColumn('datestamp_source', f.lit('labour'))
msds_demo = msds_demo.select('person_id_mother_deid','uniqpregid', f.col('AntenatalAppDate').alias("datestamp")).dropDuplicates().withColumn('demo', f.lit(1)).withColumn('datestamp_source', f.lit('demo'))
# msds_diag = msds_diag.select('person_id_mother_deid','uniqpregid', f.col('DiagnosisDate').alias("datestamp")).dropDuplicates().withColumn('diag', f.lit(1)).withColumn('datestamp_source', f.lit('diag'))
msds_care = msds_care.select('person_id_mother_deid','uniqpregid', f.col('CContactDate').alias("datestamp")).dropDuplicates().withColumn('care', f.lit(1)).withColumn('datestamp_source', f.lit('care'))
msds_assessments = msds_assessments.select('person_id_mother_deid','uniqpregid', f.col('CompDate').alias("datestamp")).dropDuplicates().withColumn('assessments', f.lit(1)).withColumn('datestamp_source', f.lit('assessments'))
# msds_findings = msds_findings.select('person_id_mother_deid','uniqpregid', f.col('FindingDate').alias("datestamp")).dropDuplicates().withColumn('findings', f.lit(1)).withColumn('datestamp_source', f.lit('findings'))
msds_hospital = msds_hospital.select('person_id_mother_deid','uniqpregid', f.col('StartDateHospProvSpell').alias("datestamp")).dropDuplicates().withColumn('hospital', f.lit(1)).withColumn('datestamp_source', f.lit('hospital'))
msds_careplan = msds_careplan.select('person_id_mother_deid','uniqpregid', f.col('CarePlanDate').alias("datestamp")).dropDuplicates().withColumn('careplan', f.lit(1)).withColumn('datestamp_source', f.lit('careplan'))


msds_baby_demo = msds_baby_demo.select('person_id_mother_deid','uniqpregid', f.col('NeonatalTransferStartDate').alias("datestamp") ).dropDuplicates().withColumn('baby_demo', f.lit(1)).withColumn('datestamp_source', f.lit('baby_demo'))
msds_baby_activities = msds_baby_activities.select('person_id_mother_deid','uniqpregid', f.col('ClinInterDateBaby' ).alias("datestamp")).dropDuplicates().withColumn('baby_activities', f.lit(1)).withColumn('datestamp_source', f.lit('baby_activities'))
msds_baby_diag = msds_baby_diag.select('person_id_mother_deid','uniqpregid', f.col('DiagnosisDate').alias("datestamp")).dropDuplicates().withColumn('baby_diag', f.lit(1)).withColumn('datestamp_source', f.lit('baby_diag'))



# COMMAND ----------

# DBTITLE 1,mother rec range
msds_mother_timeline = (msds_labour.select('person_id_mother_deid','uniqpregid', 'datestamp' , 'datestamp_source')
               .union(msds_demo.select('person_id_mother_deid','uniqpregid', 'datestamp', 'datestamp_source' ) )
# the datestamp field for msds_diag includes many historic records so should not be included               
#               .union(msds_diag.select('person_id_mother_deid','uniqpregid', 'datestamp', 'datestamp_source'))
               .union(msds_care.select('person_id_mother_deid','uniqpregid', 'datestamp', 'datestamp_source'))
               .union(msds_assessments.select('person_id_mother_deid','uniqpregid', 'datestamp', 'datestamp_source'))
# the datestamp field for msds_findings includes many historic records so should not be included          
#               .union(msds_findings.select('person_id_mother_deid','uniqpregid', 'datestamp', 'datestamp_source'))
               .union(msds_hospital.select('person_id_mother_deid','uniqpregid', 'datestamp', 'datestamp_source'))
               .union(msds_careplan.select('person_id_mother_deid','uniqpregid', 'datestamp', 'datestamp_source'))
               .where(f.col("person_id_mother_deid").isNotNull())
               .where(f.col("uniqpregid").isNotNull())
               .where(f.col("datestamp").isNotNull())
               .sort('person_id_mother_deid' , 'uniqpregid', 'datestamp')
)

msds_mother_timeline.cache()

# COMMAND ----------

from pyspark.sql import Window
w = Window.partitionBy("uniqpregid")

msds_mother_recordrange = (msds_mother_timeline
                          .where(f.col("datestamp").isNotNull())
#                          .where(f.col('datestamp') >= "2018-01-01")
                          .dropDuplicates(["uniqpregid", "datestamp"])
                          .withColumn('earliest_mother_rec', f.min('datestamp').over(w))
                          .withColumn('latest_mother_rec', f.max('datestamp').over(w))
                          .where(f.col('earliest_mother_rec') == f.col("datestamp"))
                          .select('person_id_mother_deid', 'uniqpregid', 'earliest_mother_rec', 'latest_mother_rec' , 'datestamp_source')
                          .dropDuplicates()
)

msds_mother_recordrange.cache()
# display(msds_record_daterange2.limit(100))

# COMMAND ----------

from pyspark.sql.functions import  datediff

tmp = ( msds_mother_recordrange
       .withColumn("datesDiff", f.datediff(f.col("latest_mother_rec"), f.col("earliest_mother_rec")))
#       .where(f.col("datesDiff") > 365)
)

if checks_on:
       tab( tmp , "datesDiff" )
       #tab( tmp , "datestamp_source" )

# COMMAND ----------

# DBTITLE 1,Earliest mother rec coverage plot
earliest_rec_coverage = (msds_mother_recordrange
                  .withColumn('earliest_mother_rec_month', f.date_format("earliest_mother_rec", 'yyyy-MM'))
                  .drop('earliest_mother_rec', 'latest_mother_rec', 'earliest_baby_rec', 'latest_baby_rec'  )
                  .dropDuplicates()
                  .groupBy('earliest_mother_rec_month', 'datestamp_source')
                    .agg(f.count(f.col('uniqpregid')).alias('cohort_recs'))
#                    .withColumn('cohort_recs_month', f.sum(f.col('cohort_recs')).over(Window.partitionBy('est_preg_start_month')))     
#                    .withColumn('msds_matched_prop', f.col('cohort_recs')/f.col('cohort_recs_month'))
                .sort(['earliest_mother_rec_month', 'datestamp_source'])
)

if checks_on:
  display(earliest_rec_coverage.limit(100))

# COMMAND ----------

# DBTITLE 1,Latest mother rec coverage plot
latest_rec_coverage = (msds_mother_recordrange
                  .withColumn('latest_mother_rec_month', f.date_format("latest_mother_rec", 'yyyy-MM'))
                  .drop('earliest_mother_rec', 'latest_mother_rec', 'earliest_baby_rec', 'latest_baby_rec'  )
                  .dropDuplicates()
                  .groupBy('latest_mother_rec_month', 'datestamp_source')
                    .agg(f.count(f.col('uniqpregid')).alias('cohort_recs'))
#                    .withColumn('cohort_recs_month', f.sum(f.col('cohort_recs')).over(Window.partitionBy('est_preg_start_month')))     
#                    .withColumn('msds_matched_prop', f.col('cohort_recs')/f.col('cohort_recs_month'))
                .sort(['latest_mother_rec_month', 'datestamp_source'])
)

if checks_on:
  display(latest_rec_coverage.limit(100))

# COMMAND ----------

# DBTITLE 1,baby rec range
msds_baby_recordrange = (msds_baby_demo.select('person_id_mother_deid','uniqpregid', 'datestamp' , 'datestamp_source')
               .union(msds_baby_activities.select('person_id_mother_deid','uniqpregid', 'datestamp', 'datestamp_source') )
               .union(msds_baby_diag.select('person_id_mother_deid','uniqpregid', 'datestamp', 'datestamp_source'))
               .where(f.col("person_id_mother_deid").isNotNull())
               .where(f.col("uniqpregid").isNotNull())
               .where(f.col("datestamp").isNotNull())
               .sort('person_id_mother_deid' , 'uniqpregid', 'datestamp')
)

from pyspark.sql import Window
w = Window.partitionBy("uniqpregid")

msds_baby_recordrange = (msds_baby_recordrange
                          .where(f.col("datestamp").isNotNull())
                          .dropDuplicates(["uniqpregid", "datestamp"])
                          .withColumn('earliest_baby_rec', f.min('datestamp').over(w))
                          .withColumn('latest_baby_rec', f.max('datestamp').over(w))
                          .where(f.col('earliest_baby_rec') == f.col("datestamp"))
                          .select('person_id_mother_deid', 'uniqpregid', 'earliest_baby_rec', 'latest_baby_rec', 'datestamp_source')
                          .dropDuplicates()
                          )

msds_baby_recordrange.cache()

#display(msds_baby_recordrange.limit(100))

# COMMAND ----------

# DBTITLE 1,Earliest Baby rec coverage plot
earliest_babyrec_coverage = (msds_baby_recordrange
                  .withColumn('earliest_baby_rec_month', f.date_format("earliest_baby_rec", 'yyyy-MM'))
                  .drop( 'earliest_baby_rec', 'latest_baby_rec'  )
                  .dropDuplicates()
                  .groupBy('earliest_baby_rec_month', 'datestamp_source')
                    .agg(f.count(f.col('uniqpregid')).alias('cohort_recs'))
                .sort(['earliest_baby_rec_month', 'datestamp_source'])
)

if checks_on:
    display(earliest_babyrec_coverage.limit(10))

# COMMAND ----------

# DBTITLE 1,Delivery date


# COMMAND ----------

# DBTITLE 1,Joining mother and baby record ranges
msds_record_daterange = (
    msds_mother_recordrange
    .select(    "person_id_mother_deid", "uniqpregid", "earliest_mother_rec", "latest_mother_rec")
    .join(     msds_baby_recordrange
          .select(  "person_id_mother_deid", "uniqpregid", "earliest_baby_rec", "latest_baby_rec" ),
    on=["person_id_mother_deid", "uniqpregid"],
    how="full")
)

msds_record_daterange.cache()

# COMMAND ----------

# DBTITLE 1,Saving
outName = f'{proj}_msds_record_daterange'

# save
msds_record_daterange.write.mode('overwrite').option("overwriteSchema", "true").saveAsTable(f'{dbc}.{outName}')

# COMMAND ----------

spark.catalog.clearCache()

# COMMAND ----------

# DBTITLE 1,reading back

if checks_on:
    maternity_dates = spark.table(f'{dbc}.{proj}_msds_record_daterange')
    maternity_dates.printSchema()

# COMMAND ----------

maternity_dates.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # 4 Read back

# COMMAND ----------

# DBTITLE 1,Reading back to save compute in latter half notebook
# maternity_flags = spark.table(f'{dbc}.{proj}_mother_baby_linkage')
maternity_dates = spark.table(f'{dbc}.{proj}_msds_record_daterange')

# COMMAND ----------

# MAGIC %md
# MAGIC # 5 Counts and ranks

# COMMAND ----------

maternity_dates.printSchema()

# COMMAND ----------

# DBTITLE 1,Pregs per mother
from pyspark.sql import Window
from pyspark.sql.functions import col, row_number

w2 = Window.partitionBy("person_id_mother_deid").orderBy(["person_id_mother_deid", "earliest_mother_rec"])
w3 = Window.partitionBy("person_id_mother_deid")

preg_ranks = ( 
       maternity_dates
        .orderBy(["person_id_mother_deid", "earliest_mother_rec"])
        .withColumn("preg_rank",  row_number().over(w2))
        .withColumn("total_preg_count", f.max("preg_rank").over(w3))
        .dropDuplicates()
        .sort("person_id_mother_deid")
        .select("person_id_mother_deid", "uniqpregid", "preg_rank", "total_preg_count" )
 )

preg_ranks.cache()

if checks_on:
       display(preg_ranks.limit(100))

# COMMAND ----------

# DBTITLE 1,Reading in babies
# msds baby demographics
msds_baby_demo_vars = ['Person_ID_Mother_DEID','uniqpregid', 'Person_ID_Baby_DEID',  'yearofbirthbaby' , 'monthofbirthbaby']

msds_baby_demo_births = spark.table(f'{dbc_old}.msds_v2_baby_demographics_all_years_archive')

msds_baby_demo_births = (
    msds_baby_demo_births
    .filter(f.col("archived_on") == tmp_archived_on)
    .select( msds_baby_demo_vars)
    .filter(f.col("Person_ID_Baby_DEID").isNotNull())
    .filter(f.col("Person_ID_Mother_DEID").isNotNull())
)

msds_baby_demo_births.cache()

# COMMAND ----------

# DBTITLE 1,Babies per preg
babies_per_preg = (
    msds_baby_demo_births
    .groupBy('uniqpregid')
    .agg(f.countDistinct('Person_ID_Baby_DEID').alias('babies_per_preg'))
)

checks_on:
    tab(babies_per_preg , 'babies_per_preg')

# COMMAND ----------

# DBTITLE 1,Babies per mother
babies_per_mother = (
    msds_baby_demo_births
    .groupBy('Person_ID_Mother_DEID')
    .agg(f.countDistinct('Person_ID_Baby_DEID').alias('totalbabies_per_mother'))
    
)
checks_on:
    tab(babies_per_mother , 'totalbabies_per_mother')

# COMMAND ----------

# DBTITLE 1,Baby rank
from pyspark.sql import Window
from pyspark.sql.functions import col, row_number

w2 = Window.partitionBy( "uniqpregid", "Person_ID_Baby_DEID",).orderBy(["Person_ID_Baby_DEID", "delivery_monthyear"])
w3 = Window.partitionBy("Person_ID_Mother_DEID").orderBy([ "delivery_monthyear"])

baby_ranks = ( 
       msds_baby_demo_births
       .withColumn('delivery_monthyear', f.concat_ws('-', f.col('yearofbirthbaby'), f.col('monthofbirthbaby'), f.lit('01')))
       .withColumn('delivery_monthyear', f.col('delivery_monthyear').cast("date"))
       .withColumn('delivery_monthyear', f.date_format(f.col('delivery_monthyear'), 'yyyy-MM'))
       .filter(f.col("delivery_monthyear").isNotNull())
       .orderBy(["Person_ID_Mother_DEID", "uniqpregid" , "delivery_monthyear"])
       .withColumn("delivery_monthyear_rank",  row_number().over(w2))
       .where(f.col("delivery_monthyear_rank") == 1)
       .withColumn("babyrank",  row_number().over(w3))
       .dropDuplicates()
       .orderBy(["Person_ID_Mother_DEID", "uniqpregid" , "delivery_monthyear"])
 )

baby_ranks.cache()

if checks_on:
       display(maternity_ranks.limit(100))

# COMMAND ----------

# DBTITLE 1,Combine
maternity_ranks = (
    preg_ranks
    .join(baby_ranks.select( 'uniqpregid', 'delivery_monthyear'), ['uniqpregid'], how = 'full')
    .join(babies_per_preg.select('uniqpregid', 'babies_per_preg'), ['uniqpregid'], how = 'full')
    .join(babies_per_mother.select('Person_ID_Mother_DEID', 'totalbabies_per_mother'), 'person_id_mother_deid' == 'Person_ID_Mother_DEID', how = 'full')
    #.join(baby_ranks.select( 'person_id_mother_deid', 'uniqpregid', 'Person_ID_Baby_DEID',  'babyrank'), ['person_id_mother_deid', 'uniqpregid'], how = 'left')
)

# COMMAND ----------

# DBTITLE 1,Save
outName = f'{proj}_msds_maternity_ranks'

# save
maternity_ranks.write.mode('overwrite').option("overwriteSchema", "true").saveAsTable(f'{dbc}.{outName}')

# COMMAND ----------

spark.catalog.clearCache()

# COMMAND ----------

# MAGIC %md
# MAGIC # 4 Read back

# COMMAND ----------

# DBTITLE 1,Reading back to save compute in latter half notebook
# maternity_flags = spark.table(f'{dbc}.{proj}_mother_baby_linkage')
maternity_ranks = spark.table(f'{dbc}.{proj}_msds_maternity_ranks')
