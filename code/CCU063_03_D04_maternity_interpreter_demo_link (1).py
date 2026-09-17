# Databricks notebook source
# MAGIC %md
# MAGIC # Maternity interpreter cohort demographic link
# MAGIC
# MAGIC Purpose: This notebook links the maternity interpreter cohort for CCU063_03 with the curated demographic table, and adds mother's age at booking from MSDS
# MAGIC
# MAGIC Authors: Majel McGranahan, Baboucarr Njie and Nuria Sanchez Clemente  with support from Lars Murdock
# MAGIC
# MAGIC
# MAGIC 

# COMMAND ----------

# MAGIC %md
# MAGIC #1. Parameters

# COMMAND ----------

# MAGIC %run "./CCU063_03-D01-parameters"

# COMMAND ----------

# MAGIC %md
# MAGIC ###Demographics table from 2024_07_23

# COMMAND ----------

checks_on = True

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Load maternity_interpreter_cohort table

# COMMAND ----------

maternity_interpreter_cohort1 = spark.table(f'{dbc}.{proj}_maternity_interpreter_cohort1b')

maternity_interpreter_cohort1.printSchema()
maternity_interpreter_cohort1 = (maternity_interpreter_cohort1
.select('person_id_mother_deid', 'uniqpregid', 'est_preg_start',  'lookback_start', 'lookback_issue_flag', 'NHS_NUMBER_interpreter', 'SNOMED_conceptId', 'SNOMED_conceptId_description', 'DATE_interpreter', 'RECORD_DATE_interpreter', 'interpreter_use', 'record_before_lookback', 'delivery_date'))

###NEED TO SELECT THE APPROPRIATE VARIABLES FROM marternity_interpreter_cohort

# COMMAND ----------

if checks_on:
    count_var(maternity_interpreter_cohort1, 'person_id_mother_deid')

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Adding demographics to MSDS interpreter cohort

# COMMAND ----------

# HDS curated assets: Demographics table
##CCU063_03 note - may need to update the date!


# Load data
demographics = spark.table(hds_curated_assets_demographic)

# COMMAND ----------

# Preparing demographics table: renaming columns and recoding sex_code column but with NHS_NUMBER_DEID_demo instead of PERSON_ID
demographics = (
    demographics
    .withColumn('sex_code', f.when(f.col('sex_code') == 'F', '1')
                .when(f.col('sex_code') == 'M', '2')
                .when(f.col('sex_code') == '9', f.lit(None))
                .otherwise(f.col('sex_code')))
    .select(f.col('person_id').alias('person_id_demo'), 
            f.col('date_of_birth').alias('DOB'),
            f.col('sex_code').alias('sex'),
            f.col('ethnicity_5_group').alias('eth5'),
            f.col('date_of_death').alias('DOD'),
            f.col('region').alias('region'),
            f.col('imd_quintile').alias('imd_quintile'),
            f.col('imd_decile').alias('imd_decile'),
            f.col('gdppr_min_date').alias('gdppr_min_date'),
            f.col('in_gdppr').alias('in_gdppr'))
    .where(f.col('person_id_demo').isNotNull()) # filtering out null person_id
)

#region as region, imd_quintile as imd_quintile, imd_decile as imd_decile, gdppr_min_date as gdppr_min_date, in_gdppr as in_gdppr  

# COMMAND ----------

##Attempting left join based on https://www.geeksforgeeks.org/pyspark-join-types-join-two-dataframes/

##check row count in each table before and after join (row count in output table will be same as left table row count - filtered lookup)

# left join on two dataframes 
maternity_interpreter_cohort2=maternity_interpreter_cohort1.join(demographics, 
               maternity_interpreter_cohort1.person_id_mother_deid == demographics.person_id_demo,  
               "left") 

#display table
display(maternity_interpreter_cohort2)
display(maternity_interpreter_cohort2.printSchema())

# COMMAND ----------

#Check numbers
if checks_on:
    count_var(maternity_interpreter_cohort2, 'person_id_mother_deid')

# COMMAND ----------

# MAGIC %md
# MAGIC #4. Filtering to participants in GDPPR

# COMMAND ----------

maternity_interpreter_cohort2 = (maternity_interpreter_cohort2
                    .where( f.col('in_gdppr') == 1)

)

# COMMAND ----------

#Check numbers
if checks_on:
    count_var(maternity_interpreter_cohort2, 'person_id_mother_deid')

# COMMAND ----------

# DBTITLE 1,Savepoint
outName = f'{proj}_maternity_interpreter_cohort_prefilter'

# save
maternity_interpreter_cohort2.write.mode('overwrite').option("overwriteSchema", "true").saveAsTable(f'{dbc}.{outName}')

# COMMAND ----------

maternity_interpreter_cohort = spark.table(f'{dbc}.{proj}_maternity_interpreter_cohort_prefilter')

# COMMAND ----------

# MAGIC %md
# MAGIC # 5. Filtering to participants whose lookback period starts after first record in GDPPR

# COMMAND ----------

maternity_interpreter_cohort2 = (
    maternity_interpreter_cohort2
    .withColumn('GDPPR_before_lookback', f.when(f.col('lookback_start') >= f.col('gdppr_min_date'), f.lit('Y')) 
                .when(f.col('lookback_start') < f.col('gdppr_min_date'), f.lit('N')) )
)

# COMMAND ----------

tab(maternity_interpreter_cohort2, 'GDPPR_before_lookback')

# COMMAND ----------

maternity_interpreter_cohort2 = (maternity_interpreter_cohort2
                    .where( f.col('GDPPR_before_lookback') == 'Y')

)

# COMMAND ----------

tab(maternity_interpreter_cohort2, 'GDPPR_before_lookback')

# COMMAND ----------

count_var(maternity_interpreter_cohort2, 'person_id_mother_deid')

# COMMAND ----------

# MAGIC %md
# MAGIC # 6. Adding age at booking from MSDS

# COMMAND ----------

# MAGIC %md
# MAGIC Load MSDS table

# COMMAND ----------

#latest date established on 24/1/2025 as 02/12/2024

#from pyspark.sql import functions as F

#latest_date = str(spark.table(f'{dbc_old}.msds_v2_demographics_booking_and_pregnancy_all_years_archive').select(F.max(F.col('archived_on') )).collect()[0][0])

#msds_demo = (spark.table(f'{dbc_old}.msds_v2_demographics_booking_and_pregnancy_all_years_archive'))

#print(latest_date)
#print(tmp_archived_on)
#tab(msds_demo, 'archived_on')

# COMMAND ----------

msds_demo= (spark.table(f'{dbc_old}.msds_v2_demographics_booking_and_pregnancy_all_years_archive')
            .filter(f.col('archived_on')== tmp_archived_on)
          #.filter(F.col('ADMIDATE') > "2018-01-01")
          #.filter(F.col)
          )

# COMMAND ----------

msds_demo= (
    msds_demo
    .select(f.col('person_id_mother_deid').alias('person_id_mother_deid2'),
            f.col('UniqPregId').alias('uniqpregid'), 
            f.col('ageatbookingmother').alias('ageatbookingmother')
))

# COMMAND ----------

#from https://sparkbyexamples.com/pyspark/pyspark-select-first-row-of-each-group/
#Will this order from smallest to largest DATE_interpreter?

from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number
w2 = Window.partitionBy("uniqpregid").orderBy(col("ageatbookingmother"))
msds_demo = (msds_demo
                          .withColumn("row",row_number().over(w2))
                          .filter(col("row") == 1)) 

# COMMAND ----------

maternity_interpreter_cohort = (maternity_interpreter_cohort2
                                 .join(msds_demo, on = 'uniqpregid', how= 'left'))

# COMMAND ----------

tab(maternity_interpreter_cohort, 'ageatbookingmother')

# COMMAND ----------

display(maternity_interpreter_cohort)
display(maternity_interpreter_cohort.printSchema())

# COMMAND ----------

if checks_on:
  count_var(maternity_interpreter_cohort, 'person_id_mother_deid')
  count_var(maternity_interpreter_cohort, 'uniqpregid')
  maternity_interpreter_cohort.printSchema()

# COMMAND ----------

# Imports
from pyspark.sql.functions import col, current_date, datediff, months_between, round, lit, expr

# COMMAND ----------

#Calculate age at pregnancy start for each pregnancy
# Using datediff()

maternity_interpreter_cohort = maternity_interpreter_cohort.withColumn("ageatpregstart1", expr("datediff(est_preg_start, DOB) / 365.25"))

# COMMAND ----------

tab(maternity_interpreter_cohort, 'ageatpregstart1')

# COMMAND ----------

#Remove implausible ages
maternity_interpreter_cohort=maternity_interpreter_cohort.withColumn('ageatpregstart', 
                                                                       f.when((f.col('ageatpregstart1') < 10), None)
                                                                       .when((f.col('ageatpregstart1') >=70), None)
                                                                       .otherwise(col("ageatpregstart1")))

# COMMAND ----------

from pyspark.sql.functions import coalesce

# COMMAND ----------

#replace null values in 'ageatpregstart' column with values from 'ageatbookingmother' column
maternity_interpreter_cohort = maternity_interpreter_cohort.withColumn('agefinal', coalesce('ageatpregstart', 'ageatbookingmother'))

# COMMAND ----------

tab(maternity_interpreter_cohort, 'agefinal')

# COMMAND ----------

# MAGIC %md
# MAGIC #7. Saving interpreter pregnancy cohort

# COMMAND ----------

outName = f'{proj}_maternity_interpreter_cohort'

# save
maternity_interpreter_cohort.write.mode('overwrite').option("overwriteSchema", "true").saveAsTable(f'{dbc}.{outName}')

# COMMAND ----------

maternity_interpreter_cohort = spark.table(f'{dbc}.{proj}_maternity_interpreter_cohort')
