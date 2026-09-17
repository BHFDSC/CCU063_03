# Databricks notebook source
# MAGIC %md
# MAGIC # Maternity interpreter cohort development
# MAGIC
# MAGIC Purpose: This notebook creates a cohort of people within GDPPR who have used an interpreter 
# MAGIC
# MAGIC Authors: Majel McGranahan, Baboucarr Njie and Nuria Sanchez Clemente and with support from Lars Murdock
# MAGIC
# MAGIC Based on ZW's 'Count of Codes in GDPPR for Migrant phenotyping'
# MAGIC
# MAGIC
# MAGIC 

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Ingesting SNOMED CODES
# MAGIC These have been mapped from READV2 to SNOMEDCT 

# COMMAND ----------

spark.catalog.clearCache()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Parameters

# COMMAND ----------

# MAGIC %run "./CCU063_03-D01-parameters"

# COMMAND ----------

checks_on = True

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md
# MAGIC #Creating the interpreter table

# COMMAND ----------

# MAGIC %run "Workspace/Shared/CCU063_03/ccu063_03/CCU063_03_codes_loader"

# COMMAND ----------

import pandas as pd
interpreter_needed = spark.createDataFrame(new_codes)

interpreter_needed.cache()

if checks_on:
    display(interpreter_needed.limit(10))
    interpreter_needed.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Using GDPPR Batch tables and filtering for the latest batch

# COMMAND ----------

#latest date established on 24/1/2025 as 02/12/2024

from pyspark.sql import functions as F

# assessing what the latest batch is
if checks_on:
    latest_date = str(spark.table('.gdppr__archive').select(F.max(F.col('archived_on') )).collect()[0][0])

# reading in gdppr for specified batch
gdppr = (spark.table('.gdppr__archive').filter(F.col('archived_on')== tmp_archived_on))

if checks_on:
    print(latest_date)
    print(tmp_archived_on)
    # tab( gdppr, 'archived_on')

# COMMAND ----------

#Check - Count number of people in GDPPR
#count_var(gdppr, 'NHS_NUMBER_DEID')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Joining new_codes to a lookup table to get snomed descriptions

# COMMAND ----------

# DBTITLE 1,attempting to rewrite sql code into pyspark for consistency
gdppr_interpr_combined = ( 
    gdppr
    .select(F.col('NHS_NUMBER_DEID').alias('NHS_NUMBER_interpreter'),
            F.col('CODE').alias('SNOMED_conceptId'),
            F.col('DATE').alias('DATE_interpreter'),
            F.col('RECORD_DATE').alias('RECORD_DATE_interpreter'))
    .join(F.broadcast(interpreter_needed
                    .select( F.col('revised_conceptid').alias('SNOMED_conceptId'),
                            F.col('conceptid_description').alias('SNOMED_conceptId_description'),
                            #F.col('Term'),
                            #F.col('gdppr_flag').alias('gdppr_flag')
                            )
                    )
        , on='SNOMED_conceptId')
    .filter(f.col('DATE_interpreter').isNotNull())
    .dropDuplicates()
)

gdppr_interpr_combined.cache()

# preserving a version in order to explore codes
gdppr_interpr_combined_long = (
        gdppr_interpr_combined
        .dropDuplicates(['NHS_NUMBER_interpreter', 'SNOMED_conceptId' , 'SNOMED_conceptId_description' ])
)





# COMMAND ----------

if checks_on:
    count_var(gdppr_interpr_combined, 'NHS_NUMBER_interpreter')

# COMMAND ----------

# MAGIC %md
# MAGIC # Filtered to first interpreter code per patient

# COMMAND ----------


from pyspark.sql import Window
w = Window.partitionBy("NHS_NUMBER_interpreter").orderBy(F.col('DATE_interpreter'))

gdppr_interpr_combined_single_row = (
    gdppr_interpr_combined
        .sort("NHS_NUMBER_interpreter", "DATE_interpreter")
        .withColumn("record_to_keep", f.min("DATE_interpreter").over(w))
        .dropDuplicates()
        .filter(f.col("DATE_interpreter") == f.col("record_to_keep"))
        .dropDuplicates(["NHS_NUMBER_interpreter"])
)

gdppr_interpr_combined_single_row.cache()

# COMMAND ----------

if checks_on:
    display(gdppr_interpr_combined.limit(10))
    count_var(gdppr_interpr_combined, 'NHS_NUMBER_interpreter')
    gdppr_interpr_combined.printSchema()

# COMMAND ----------

if checks_on:
    display(gdppr_interpr_combined_single_row.sort('NHS_NUMBER_interpreter', 'DATE_interpreter').limit(100))
    count_var(gdppr_interpr_combined_single_row, 'NHS_NUMBER_interpreter')
    gdppr_interpr_combined_single_row.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #Save interpreter cohort table

# COMMAND ----------

outName = f'{proj}_gdppr_interpr_combined'

# save
gdppr_interpr_combined_single_row.write.mode('overwrite').option("overwriteSchema", "true").saveAsTable(f'{dbc}.{outName}')


# COMMAND ----------

spark.catalog.clearCache()

# COMMAND ----------

# MAGIC %md
# MAGIC # Read back

# COMMAND ----------

gdppr_interpr_combined_readback = spark.table(f'{dbc}.{proj}_gdppr_interpr_combined')

# COMMAND ----------

if checks_on:
    display(gdppr_interpr_combined_readback.sort('NHS_NUMBER_interpreter', 'DATE_interpreter').limit(10))
    count_var(gdppr_interpr_combined_readback, 'NHS_NUMBER_interpreter')
    gdppr_interpr_combined_readback.printSchema()
