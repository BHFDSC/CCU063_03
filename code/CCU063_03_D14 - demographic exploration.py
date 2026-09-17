# Databricks notebook source
# MAGIC %md
# MAGIC ##Creating tables with demographic data for output
# MAGIC
# MAGIC Purpose - to summarise the demographic characteristics of the cohort and compare demographic characteristics between interpreter users and non-interpreter users
# MAGIC
# MAGIC Authors - Majel McGranahan supported by Lars Murdock. Some code from CCU085_01-D14.
# MAGIC
# MAGIC Reviewed - Not reviewed, needs cleaning up by MM

# COMMAND ----------

# MAGIC %md
# MAGIC #0 Parameters

# COMMAND ----------

# MAGIC %run "./CCU063_03-D01-parameters"

# COMMAND ----------

checks_on = True

# COMMAND ----------

# MAGIC %md
# MAGIC #1. Load maternity interpreter cohort

# COMMAND ----------

maternity_interpreter_cohort = spark.table(f'{dbc}.{proj}_cohort_ae_interaction_counts')

maternity_interpreter_cohort.printSchema()
maternity_interpreter_cohort = (maternity_interpreter_cohort
.select('person_id_mother_deid', 'uniqpregid', 'est_preg_start',  'lookback_start', 'lookback_issue_flag', 'NHS_NUMBER_interpreter', 'SNOMED_conceptId', 'SNOMED_conceptId_description', 'DATE_interpreter', 'RECORD_DATE_interpreter', 'person_id_demo', 'Dob', 'eth5', 'region', 'imd_quintile', 'imd_decile', 'in_gdppr', 'gdppr_min_date', 'interpreter_use', 'record_before_lookback', 'ageatbookingmother', 'delivery_date', 'agefinal', 'folicacid', 'ovsvischcat', 'ovsvischcatappdate', 'complexsocialfactors', 'gestagebooking', 'gestagebookingweeks', 'previouslivebirths', 'previousstillbirths', 'previouslosseslessthan24weeks', 'parity', 'nulliparous', 'prev_loss', 'prev_preg', 'booking_after_10weeks', 'fact_of_gp_interaction', 'fact_of_ae_interaction')
        )

# COMMAND ----------

tab(maternity_interpreter_cohort, 'agefinal')

# COMMAND ----------

##check
if checks_on:
    count_var(maternity_interpreter_cohort, 'person_id_mother_deid')

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

if checks_on:
    count_var(maternity_interpreter_cohort, 'uniqpregid')
    maternity_interpreter_cohort.printSchema()

# COMMAND ----------

if checks_on:
    count_var(maternity_interpreter_cohort, 'NHS_NUMBER_interpreter')

# COMMAND ----------

display(maternity_interpreter_cohort)

# COMMAND ----------

#from https://sparkbyexamples.com/pyspark/pyspark-select-first-row-of-each-group/
#Will this order from smallest to largest DATE_interpreter?

#from pyspark.sql.window import Window
#from pyspark.sql.functions import col, row_number
#w2 = Window.partitionBy("uniqpregid").orderBy(col("ageatbookingmother"))
#maternity_interpreter_cohort = ( maternity_interpreter_cohort
#                          .withColumn("row",row_number().over(w2))
#                          .filter(col("row") == 1))

# COMMAND ----------

#count_var(maternity_interpreter_cohort, 'uniqpregid')

# COMMAND ----------

#maternity_interpreter_cohort=maternity_interpreter_cohort.select(col("ageatbookingmother").cast('int').alias("age"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Converting age to continuous variable

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

maternity_interpreter_cohort1 = (maternity_interpreter_cohort
.select (col('agefinal').cast('float').alias("age"), 'person_id_mother_deid', 'eth5', 'region', col('imd_quintile').cast('string').alias('imd_quintile'), 'interpreter_use', 'ovsvischcat', 'folicacid', 'complexsocialfactors', col('gestagebookingweeks').cast('int').alias('gestagebookingweeks'), col('previouslivebirths').cast('int').alias('previouslivebirths'), col('previousstillbirths').cast('int').alias('previousstillbirths'), col('parity').cast('int').alias('parity'), 'nulliparous', 'prev_loss', 'prev_preg', 'booking_after_10weeks', 'fact_of_gp_interaction', 'fact_of_ae_interaction'))

# COMMAND ----------

import pyspark.sql.functions as f
import pyspark.sql.types as t
import pandas as pd
import re
from pyspark.sql import Window
import seaborn as sns
import matplotlib as mpl

# COMMAND ----------

tab(maternity_interpreter_cohort1, 'age')

# COMMAND ----------

# MAGIC %md
# MAGIC #Summary tables overall

# COMMAND ----------

maternity_interpreter_cohort3 = (maternity_interpreter_cohort1
.select('eth5', 'imd_quintile', 'interpreter_use', 'age', 'folicacid', 'ovsvischcat', 'complexsocialfactors', 'booking_after_10weeks', 'region', 'prev_preg', 'fact_of_gp_interaction', 'fact_of_ae_interaction'))

# COMMAND ----------

# The table_summ() function produces 3 summary output tables (as described below)
out_ssnap_continuous_summ, out_ssnap_categorical_summ, out_ssnap_categorical_freq = table_summ(_data=maternity_interpreter_cohort3, _name='out_table')

# COMMAND ----------

display(out_ssnap_continuous_summ) 

# COMMAND ----------

display(out_ssnap_categorical_summ) 
display(out_ssnap_categorical_freq)

# COMMAND ----------

print(type(out_ssnap_categorical_freq))
display(out_ssnap_categorical_freq)

# COMMAND ----------

# MAGIC %md
# MAGIC #rounding to nearest 10

# COMMAND ----------

# DBTITLE 1,SDC - Statistical Disclosure Control
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

# small additional code to convert from pandas dataframe to a spark dataframe (and converting to the correct col type)
out_ssnap_categorical_freq = spark.createDataFrame(out_ssnap_categorical_freq).withColumn("freq_n", f.col("freq_n").cast(IntegerType()))

# which col or cols to apply to (ie. which cols have the counts that need disclosure control)
cols = ['freq_n']


for i, var in enumerate(cols):
  typ = dict(out_ssnap_categorical_freq.dtypes)[var]
  #print(i, var, typ)
  assert str(typ) in('bigint')
  assert out_ssnap_categorical_freq.where(f.col(var)<0).count() == 0
  out_ssnap_categorical_freq_sdc = (
      out_ssnap_categorical_freq         
      .withColumn(var,
                  f.when(f.col(var) == 0, 0)
                     .when(f.col(var) < 10, 10)
                     .when(f.col(var) >= 10, 5*f.round(f.col(var)/5))
                    )
        )

# COMMAND ----------

# DBTITLE 1,View the output

display(out_ssnap_categorical_freq_sdc)

# COMMAND ----------

# MAGIC %md
# MAGIC #Summary tables for interpreter users

# COMMAND ----------

maternity_interpreter_cohort4 = (maternity_interpreter_cohort3
                    .where( f.col('interpreter_use') == "yes")

)

# COMMAND ----------

count_var(maternity_interpreter_cohort4, 'interpreter_use')

# COMMAND ----------

# The table_summ() function produces 3 summary output tables (as described below)
out_ssnap_continuous_summ, out_ssnap_categorical_summ, out_ssnap_categorical_freq1 = table_summ(_data=maternity_interpreter_cohort4, _name='out_table')

# COMMAND ----------

display(out_ssnap_continuous_summ) 

# COMMAND ----------

display(out_ssnap_categorical_summ) 
display(out_ssnap_categorical_freq1)

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType

# small additional code to convert from pandas dataframe to a spark dataframe (and converting to the correct col type)
out_ssnap_categorical_freq1 = spark.createDataFrame(out_ssnap_categorical_freq1).withColumn("freq_n", f.col("freq_n").cast(IntegerType()))

# which col or cols to apply to (ie. which cols have the counts that need disclosure control)
cols = ['freq_n']


for i, var in enumerate(cols):
  typ = dict(out_ssnap_categorical_freq1.dtypes)[var]
  #print(i, var, typ)
  assert str(typ) in('bigint')
  assert out_ssnap_categorical_freq1.where(f.col(var)<0).count() == 0
  out_ssnap_categorical_freq1_sdc = (
      out_ssnap_categorical_freq1         
      .withColumn(var,
                  f.when(f.col(var) == 0, 0)
                     .when(f.col(var) < 10, 10)
                     .when(f.col(var) >= 10, 5*f.round(f.col(var)/5))
                    )
        )

# COMMAND ----------

display(out_ssnap_categorical_freq1_sdc)

# COMMAND ----------

# MAGIC %md
# MAGIC #Summary tables for non-interpreter users

# COMMAND ----------

maternity_interpreter_cohort5 = (maternity_interpreter_cohort3
                    .where( f.col('interpreter_use') == "no")

)

# COMMAND ----------

count_var(maternity_interpreter_cohort5, 'interpreter_use')

# COMMAND ----------

# The table_summ() function produces 3 summary output tables (as described below)
out_ssnap_continuous_summ, out_ssnap_categorical_summ, out_ssnap_categorical_freq = table_summ(_data=maternity_interpreter_cohort5, _name='out_table')

# COMMAND ----------

display(out_ssnap_continuous_summ) 

# COMMAND ----------

display(out_ssnap_categorical_summ) 
display(out_ssnap_categorical_freq)

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType

# small additional code to convert from pandas dataframe to a spark dataframe (and converting to the correct col type)
out_ssnap_categorical_freq = spark.createDataFrame(out_ssnap_categorical_freq).withColumn("freq_n", f.col("freq_n").cast(IntegerType()))

# which col or cols to apply to (ie. which cols have the counts that need disclosure control)
cols = ['freq_n']


for i, var in enumerate(cols):
  typ = dict(out_ssnap_categorical_freq.dtypes)[var]
  #print(i, var, typ)
  assert str(typ) in('bigint')
  assert out_ssnap_categorical_freq.where(f.col(var)<0).count() == 0
  out_ssnap_categorical_freq_sdc = (
      out_ssnap_categorical_freq         
      .withColumn(var,
                  f.when(f.col(var) == 0, 0)
                     .when(f.col(var) < 10, 10)
                     .when(f.col(var) >= 10, 5*f.round(f.col(var)/5))
                    )
        )

# COMMAND ----------

display(out_ssnap_categorical_freq_sdc)