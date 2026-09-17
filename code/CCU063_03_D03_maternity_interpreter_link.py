# Databricks notebook source
# MAGIC %md
# MAGIC # Maternity interpreter cohort development
# MAGIC
# MAGIC Purpose: This notebook links the maternity cohort for CCU063_03 with interpreter codes 
# MAGIC
# MAGIC Authors: Majel McGranahan, Baboucarr Njie and Nuria Sanchez Clemente with support from Lars Murdock
# MAGIC
# MAGIC
# MAGIC
# MAGIC 

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Parameters

# COMMAND ----------

# MAGIC %run "./CCU063_03-D01-parameters"

# COMMAND ----------

checks_on = True

# COMMAND ----------

# MAGIC %md
# MAGIC #2. Loading the interpreter table

# COMMAND ----------

gdppr_interpr_combined = spark.table(f'{dbc}.{proj}_gdppr_interpr_combined')

gdppr_interpr_combined.printSchema()
gdppr_interpr_combined = (gdppr_interpr_combined
.select('NHS_NUMBER_interpreter', 'DATE_interpreter', 'RECORD_DATE_interpreter', 'SNOMED_conceptId', 'SNOMED_conceptId_description'))




# COMMAND ----------

if checks_on:
    count_var(gdppr_interpr_combined, 'NHS_NUMBER_interpreter')

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Load MSDS cohort table

# COMMAND ----------

maternity_cohort = spark.table(f'{dbc}.{proj}_maternity_cohort')

maternity_cohort.printSchema()
maternity_cohort = (maternity_cohort
.select('person_id_mother_deid', 'uniqpregid', 'est_preg_start',  'lookback_start', 'lookback_issue_flag'))

# COMMAND ----------

if checks_on:
    count_var(maternity_cohort, 'person_id_mother_deid')
    count_var(maternity_cohort, 'uniqpregid')

# COMMAND ----------

# MAGIC %md
# MAGIC #Filter out those with lookback issue flag 
# MAGIC This means they have a pregnancy in the year preceeding their index pregnancy but before the study start date

# COMMAND ----------

maternity_cohort = maternity_cohort.filter(maternity_cohort.lookback_issue_flag != 1.0)

# COMMAND ----------

if checks_on:
    count_var(maternity_cohort, 'person_id_mother_deid')
    count_var(maternity_cohort, 'uniqpregid')
    display(maternity_cohort.limit(10))
    tab(maternity_cohort, 'est_preg_start')
    tab(maternity_cohort, 'lookback_start')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Joining MSDS cohort table to interpreter codes

# COMMAND ----------

##Attempting left join based on https://www.geeksforgeeks.org/pyspark-join-types-join-two-dataframes/

##check row count in each table before and after join (row count in output table will be same as left table row count - filtered lookup)

# left join on two dataframes 
maternity_interpreter_cohort1=maternity_cohort.join(gdppr_interpr_combined, 
               maternity_cohort.person_id_mother_deid == gdppr_interpr_combined.NHS_NUMBER_interpreter,  
               "left") 


#display variables
display(maternity_interpreter_cohort1.printSchema())

# COMMAND ----------

#Determine earliest date of interpreter recording
from pyspark.sql.functions import min, col
earliest_date = maternity_interpreter_cohort1.select(min(col("DATE_interpreter")).alias("earliest_date"))
display(earliest_date)

# COMMAND ----------

if checks_on:
    count_var(maternity_interpreter_cohort1, 'person_id_mother_deid')
    count_var(maternity_interpreter_cohort1, 'uniqpregid')
    display(maternity_interpreter_cohort1.limit(100))


# COMMAND ----------

# MAGIC %md
# MAGIC #4. Adding interpreter use column

# COMMAND ----------

maternity_interpreter_cohort1=maternity_interpreter_cohort1.withColumn('interpreter_use', f.when(f.col('SNOMED_conceptId').isNotNull(), 'yes').otherwise('no'))

# COMMAND ----------

tab(maternity_interpreter_cohort1, 'interpreter_use')

# COMMAND ----------

# MAGIC %md
# MAGIC #5. Add record before lookback column

# COMMAND ----------

maternity_interpreter_cohort1=maternity_interpreter_cohort1.withColumn('record_before_lookback', 
                                                                       f.when(f.col('DATE_interpreter') < f.col ('lookback_start'), 'yes')
                                                                       .when(f.col('DATE_interpreter') > f.col ('lookback_start'), 'no')
                                                                       .otherwise(None))

# COMMAND ----------

# MAGIC %md
# MAGIC # 6. Adding delivery date from MSDS baby demo table

# COMMAND ----------

msds_baby_demo= (spark.table(f'{dbc_old}.msds_v2_baby_demographics_all_years_archive')
           .filter(f.col('archived_on')== tmp_archived_on)
          #.filter(F.col('ADMIDATE') > "2018-01-01")
          #.filter(F.col)
          )

# COMMAND ----------

#select appropriate columns
msds_baby_demo= (
    msds_baby_demo
    .select(f.col('Monthofbirthbaby').alias('Monthofbirthbaby'),
            f.col('Yearofbirthbaby').alias('Yearofbirthbaby'),
            f.col('Dayofbirthbaby').alias('Dayofbirthbaby'),
            f.col('uniqpregid').alias('uniqpregid')
))

# COMMAND ----------

#Convert seperate date variables to single 'delivery_date' variable

#code from https://stackoverflow.com/questions/60954146/how-to-create-date-from-year-month-and-day-in-pyspark


from pyspark.sql.functions import *

msds_baby_demo= (
    msds_baby_demo
    .withColumn("delivery_date",concat_ws("-",col("Yearofbirthbaby"),col("Monthofbirthbaby"),col("Dayofbirthbaby")).cast("date")))

# COMMAND ----------

msds_baby_demo= (
    msds_baby_demo
    .select(f.col('uniqpregid').alias('uniqpregid'),
            f.col('delivery_date').alias('delivery_date')
))

# COMMAND ----------

#from https://sparkbyexamples.com/pyspark/pyspark-select-first-row-of-each-group/
#Will this order from smallest to largest date of birth?

from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number
w2 = Window.partitionBy("uniqpregid").orderBy(col("delivery_date"))
msds_baby_demo = (msds_baby_demo
                          .withColumn("row",row_number().over(w2))
                          .filter(col("row") == 1)) 

# COMMAND ----------

msds_baby_demo = msds_baby_demo.drop('row')

# COMMAND ----------

display(msds_baby_demo)

# COMMAND ----------

# left join on two dataframes 
maternity_interpreter_cohort1 = (maternity_interpreter_cohort1
                                 .join(msds_baby_demo, on = ['uniqpregid'], how= 'left'))


#display table
display(maternity_interpreter_cohort1)
display(maternity_interpreter_cohort1.printSchema())

# COMMAND ----------

#Commented out due to function not working
#if checks_on:
#    count_var(maternity_interpreter_cohort1, "uniqpregid")
#    count_var(maternity_interpreter_cohort1, "person_id_mother_deid")

# COMMAND ----------

#Count distinct number of uniqpregid because count_var not working
df2 = maternity_interpreter_cohort1.select("uniqpregid").distinct()
print(df2.count())


# COMMAND ----------

#count number of rows since count_var not workin
print(maternity_interpreter_cohort1.count())

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #7. Saving interpreter pregnancy cohort

# COMMAND ----------

maternity_interpreter_cohort1.printSchema()

# COMMAND ----------

outName = f'{proj}_maternity_interpreter_cohort1b'

# save
maternity_interpreter_cohort1.write.mode('overwrite').option("overwriteSchema", "true").saveAsTable(f'{dbc}.{outName}')
maternity_interpreter_cohort1 = spark.table(f'{dbc}.{proj}_maternity_interpreter_cohort1b')
