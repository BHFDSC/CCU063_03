# Databricks notebook source
# MAGIC %md
# MAGIC ## Filtering from the maternity matrix for the project specific cohort
# MAGIC
# MAGIC **Purpose:** The cohort in this project is largely based on mothers and their records shortly prior to pregnancy. Maternity records will be needed to help define this cohort, and capture key information relevant to the study. The maternity records will come from two possible sources - msds and hes_mat. This notebook draws from the maternity matrix already constructed to then filter down to the relevant patients and information for this project. 
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
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #0 Parameters

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

# -----------------------------------------------------------------------------
# Databases
# -----------------------------------------------------------------------------
db = ''
dbc_old = f'{db}_collab'
dbc = ''
dsa = f''

proj = 'ccu063_03'
checks_on = False

# dataframe of required datasets

# MSDS
# Using newer data (Feb 2024 batch), and a newer version (v2 which should include data loss fixes)
tmp_archived_on = '2024-10-24'
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

hds_curated_assets_demographic = f'{dsa}.hds_curated_assets__demographics_2024_07_23'

# CCU018_02 cohort - TO DELETE - this is simply a reference point to how other projects have curated their mat linkage
# ccu018_02_cohort = spark.table(f'{dbc}.{proj}_out_cohort')

# COMMAND ----------

# MAGIC %md
# MAGIC # Reading in and assembling maternity matrix

# COMMAND ----------

# maternity_flags = spark.table(f'{dbc}.{proj}_mother_baby_linkage')
maternity_dates = spark.table(f'{dbc}.{proj}_msds_record_daterange')
maternity_start = spark.table(f'{dbc}.{proj}_msds_preg_start')
maternity_ranks = spark.table(f'{dbc}.{proj}_msds_maternity_ranks')


# COMMAND ----------

count_var(maternity_dates, "person_id_mother_deid")
count_var(maternity_start, "person_id_mother_deid")
count_var(maternity_ranks, "person_id_mother_deid")

count_var(maternity_dates, "uniqpregid")
count_var(maternity_start, "uniqpregid")
count_var(maternity_ranks, "uniqpregid")

# COMMAND ----------

maternity_start.printSchema()

# COMMAND ----------

if checks_on:
    display(maternity_start.sort("est_preg_start", "priority"), limit=100)
    tab( maternity_start, "priority")

count_var( maternity_start, "uniqpregid")
count_var( maternity_start, "person_id_mother_deid")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading in maternity_demo table to determine age at booking

# COMMAND ----------

# Load data
demographics = spark.table(hds_curated_assets_demographic)

# COMMAND ----------

# Preparing demographics table: renaming columns and recoding sex_code column but with person_id_mother_deid instead of PERSON_ID
demographics = (
    demographics
    .select(f.col('person_id').alias('person_id_mother_deid'), 
            f.col('date_of_birth').alias('DOB'))
    .where(f.col('person_id_mother_deid').isNotNull()) # filtering out null person_id
)

# COMMAND ----------

msds_demo= (spark.table(f'{dbc_old}.msds_v2_demographics_booking_and_pregnancy_all_years_archive')
            .filter(f.col('archived_on')== tmp_archived_on)
          #.filter(F.col('ADMIDATE') > "2018-01-01")
          #.filter(F.col)
          )

# COMMAND ----------

# DBTITLE 1,Majel added
msds_demo= (
    msds_demo
    .select(f.col('person_id_mother_deid').alias('person_id_mother_deid2'),
            f.col('UniqPregId').alias('uniqpregid'), 
            f.col('ageatbookingmother').alias('ageatbookingmother')
))

# COMMAND ----------

# DBTITLE 1,Majel added
#select first age at booking for each mother
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number
w2 = Window.partitionBy("uniqpregid").orderBy(col("ageatbookingmother"))
msds_demo = (msds_demo
                          .withColumn("row",row_number().over(w2))
                          .filter(col("row") == 1)) 

# COMMAND ----------

# DBTITLE 1,Majel added
#Join DOB from demographics with maternity_start table
maternity_start = (maternity_start
                                 .join(demographics, on = 'person_id_mother_deid', how= 'left'))

# COMMAND ----------

# Imports
from pyspark.sql.functions import col, current_date, datediff, months_between, round, lit, expr

# COMMAND ----------

#Calculate age at pregnancy start for each pregnancy
# Using datediff()

maternity_start = maternity_start.withColumn("ageatpregstart1", expr("datediff(est_preg_start, DOB) / 365.25"))

# COMMAND ----------

tab(maternity_start, 'ageatpregstart1')

# COMMAND ----------

#Remove implausible ages
maternity_start=maternity_start.withColumn('ageatpregstart', 
                                                                       f.when((f.col('ageatpregstart1') < 10), None)
                                                                       .when((f.col('ageatpregstart1') >=70), None)
                                                                       .otherwise(col("ageatpregstart1")))

# COMMAND ----------

tab(maternity_start, 'ageatpregstart')

# COMMAND ----------

#Join age at booking from MSDS demographics demographics with maternity_start table
maternity_start = (maternity_start
                                 .join(msds_demo, on = 'uniqpregid', how= 'left'))

# COMMAND ----------

tab(maternity_start, 'ageatbookingmother')

# COMMAND ----------

#Remove implausable ages
maternity_start=maternity_start.withColumn('ageatbookingmother', 
                                                                       f.when((f.col('ageatbookingmother') < 10), None)
                                                                       .when((f.col('ageatbookingmother') >=70), None)
                                                                       .otherwise(col('ageatbookingmother')))

# COMMAND ----------

from pyspark.sql.functions import coalesce

# COMMAND ----------

#replace null values in 'ageatpregstart' column with values from 'ageatbookingmother' column
maternity_start = maternity_start.withColumn('agefinal', coalesce('ageatpregstart', 'ageatbookingmother'))

# COMMAND ----------

tab(maternity_start, 'agefinal')

# COMMAND ----------

#Summarise the three sources for age to compare
maternity_start.select("ageatbookingmother", "agefinal", "ageatpregstart").summary().show()

# COMMAND ----------

# DBTITLE 1,Majel added
count_var( maternity_start, "uniqpregid")
count_var( maternity_start, "person_id_mother_deid")

# COMMAND ----------

# DBTITLE 1,Majel added
maternity_start.printSchema()

# COMMAND ----------

# DBTITLE 1,coverage over time- by start date
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

# COMMAND ----------

maternity_dates.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # ccu063_03 maternity cohort

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Specs
# MAGIC - filter down to pregs starting 1 march 2019 up until 29 feb 2024.
# MAGIC - limit cohort to women aged 18-49 years
# MAGIC - use only msds for now, but read in hesmat to check when that's ready
# MAGIC - use first preg only in study period
# MAGIC - use latest start date of prev preg to check whether some are overlapping with the look back
# MAGIC - 1 year look back only so field for this

# COMMAND ----------

# DBTITLE 1,project params
early_cutoff = '2019-03-1'
late_cutoff = '2024-02-29'

# COMMAND ----------

# DBTITLE 1,ccu063_03 maternity cohort
from pyspark.sql import Window
from pyspark.sql.functions import col, row_number

w3 = Window.partitionBy("person_id_mother_deid")

maternity_cohort = (maternity_start
                    .where( f.col('agefinal') >= 18.0)
                    .where( f.col('agefinal') < 50.0)
                    .where(f.col('est_preg_start') >= early_cutoff)
                    .where(f.col('est_preg_start') <= late_cutoff)
                    .join(maternity_dates, on=['person_id_mother_deid', 'uniqpregid'], how='inner')
                    .join(maternity_ranks, on=['person_id_mother_deid', 'uniqpregid'], how='inner')
                    .withColumn("first_preg_in_study", f.min("preg_rank").over(w3))
                    .where( f.col('first_preg_in_study') == f.col('preg_rank'))
                    .withColumn("lookback_start", f.date_sub("est_preg_start", 365))
                    .drop(*['ageatpregstart', 'person_id_mother_deid2', 'ageatbookingmother', 'row', 'agefinal', 'DOB'])
)

# Some women will have a pregnancy that preceeds the study window one, but didn't conclude more than a year prior. This would interfere with the lookback period of the studied pregnancy. We therefore need to flag and probably exclude these. This code locates the end date of the prior pregnancy

prior_pregnancy = (
    maternity_cohort
    .withColumn("preg_rank", f.col("preg_rank") - 1  )
    .where( f.col('preg_rank') > 0 )
    .select('person_id_mother_deid',  'preg_rank')
    .join(maternity_ranks, on=['person_id_mother_deid', 'preg_rank'], how='inner')
    .join(maternity_dates, on=['person_id_mother_deid', 'uniqpregid'], how='inner')
    .select( 'person_id_mother_deid',  'latest_mother_rec')
    .withColumnRenamed(  'latest_mother_rec', 'prev_preg_end')
)  

# rejoining this prior preg info with the studied pregnancy to flag any issues                  
maternity_cohort = (
    maternity_cohort
    .join(prior_pregnancy, on='person_id_mother_deid', how='left')
    .withColumn("lookback_issue_flag", f.when(f.col('prev_preg_end') > f.col('lookback_start'), f.lit(1)).otherwise(f.lit(0)))
)


# COMMAND ----------

display(maternity_cohort.sort('lookback_issue_flag', ascending=False), limit=100)

tab(maternity_cohort, 'lookback_issue_flag')

# COMMAND ----------

count_var(maternity_cohort, 'person_id_mother_deid')

# COMMAND ----------

count_var(maternity_cohort, 'uniqpregid')

# COMMAND ----------

# MAGIC %md
# MAGIC # Saving cohort 
# MAGIC

# COMMAND ----------

outName = f'{proj}_maternity_cohort'

# save
maternity_cohort.write.mode('overwrite').option("overwriteSchema", "true").saveAsTable(f'{dbc}.{outName}')

# COMMAND ----------

# MAGIC %md
# MAGIC #   Reading back

# COMMAND ----------

maternity_cohort = spark.table(f'{dbc}.{proj}_maternity_cohort')

# COMMAND ----------

if checks_on:
  count_var(maternity_cohort, 'person_id_mother_deid')
  count_var(maternity_cohort, 'uniqpregid')
  maternity_cohort.printSchema()

# COMMAND ----------

# DBTITLE 1,Cohort viz
preg_start_coverage = (maternity_cohort
                  .withColumn('est_preg_start_month', f.date_format("est_preg_start", 'yyyy-MM'))
                  .drop('est_preg_start')
                  .dropDuplicates()
                  .groupBy('est_preg_start_month', 'preg_rank', 'lookback_issue_flag')
                    .agg(f.count(f.col('uniqpregid')).alias('cohort_recs'))
#                    .withColumn('cohort_recs_month', f.sum(f.col('cohort_recs')).over(Window.partitionBy('est_preg_start_month')))     
#                    .withColumn('msds_matched_prop', f.col('cohort_recs')/f.col('cohort_recs_month'))
                .sort(['est_preg_start_month', 'preg_rank'])
)

# if checks_on:
display(preg_start_coverage)

# COMMAND ----------

# MAGIC %md
# MAGIC # Remaining issues to investigate
# MAGIC
# MAGIC There's a rough cohort now to use from the maternity side. Some outstanding issues do need to be investigated
# MAGIC
# MAGIC - dump codes used for unipregid
# MAGIC - multiple 'first' preg (same mother but concurrent pregnancies with diff id fields), possible issue with the 'start date' drawn from diagnosis date fields/tables
# MAGIC - lookback flag issue present still appearing in later years/months of the study period. Likely an issue with how the 'latest mother date' is derived

# COMMAND ----------

# MAGIC %md
# MAGIC # Code for Majel

# COMMAND ----------

# maternity_cohort = spark.table(f'{dbc}.{proj}_maternity_cohort')

# maternity_cohort.printSchema()

#maternity_cohort = (maternity_cohort
# .select('person_id_mother_deid', 'uniqpregid', 'est_preg_start',  'lookback_start', 'lookback_issue_flag'))

# COMMAND ----------

