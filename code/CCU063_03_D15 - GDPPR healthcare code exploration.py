# Databricks notebook source
# MAGIC %md
# MAGIC ##Exploring GDPPR healthcare use codes among maternity interpreter cohort 
# MAGIC
# MAGIC Purpose - to explore reasons for GDPPR attendance in year preconception within maternity interpreter cohort
# MAGIC
# MAGIC Dependencies - this notebook requires CCU063_03-D01, CCU063_03-D02 and CCU063_03-D03 to have saved their output tables in order to run
# MAGIC
# MAGIC Authors - Majel McGranahan supported by Lars Murdock
# MAGIC
# MAGIC Reviewed - Not reviewed

# COMMAND ----------

# MAGIC %md
# MAGIC #0 Parameters

# COMMAND ----------

# MAGIC %run "/Workspace/Shared/CCU063_03/ccu063_03/CCU063_03-D01-parameters"

# COMMAND ----------

checks_on = False

# COMMAND ----------

# MAGIC %md
# MAGIC #1. Load GP interpreter maternity cohort table

# COMMAND ----------

maternity_interpreter_GDPPR = spark.table(f'{dbc}.{proj}_gp_interaction_details')

# COMMAND ----------

# MAGIC %md
# MAGIC #2. Exploring SNOMED codes

# COMMAND ----------

### Preparing the translation of codes from icd10 and snomed ref tables

icd10_ref = spark.table('.icd10_codes').filter(f.col("VERSION") == "5th ed")
snomed_ref = (spark.table(".snomed_sct2_description_full")
    .filter(f.col("typeID") == "900000000000003001") # typeID refers to the lead meaning or a synonym. Dropping the synonyms to help get to unique meaning level.
    .orderBy(["active", "effectiveTime"] , ascending = False) # sorting to active (over not) and newest
    .dropDuplicates(['conceptId'])
)

count_var(icd10_ref, "CODE")
count_var(snomed_ref, "conceptId")

combined_ref_table = (icd10_ref
        .withColumn("CODE", f.col("ALT_CODE"))
        .select( "CODE", "DESCRIPTION"  )
        .union( snomed_ref.withColumn( "CODE" , f.col("conceptId") ).withColumn( "DESCRIPTION" , f.col("term") ).select( "CODE", "DESCRIPTION") )
)

display(combined_ref_table.head(10))
count_var(combined_ref_table, "CODE")

# COMMAND ----------

tmp = (maternity_interpreter_GDPPR
       .select('uniqpregid',   'DATE', 'CODE', 'Person_ID_Mother_DEID')
       .dropDuplicates(['uniqpregid', 'DATE', 'CODE'])
       .withColumn('DATE', f.date_format(f.col('DATE'), 'yyyy-MM'))
       .withColumn('CODE', f.regexp_replace(f.col('CODE'), "[^\w\s]+" , ""))
       .sort('uniqpregid')
       .join( combined_ref_table, on =  ["CODE"], how = "left" )
       .withColumn("FullName", f.concat(f.col("CODE"), f.lit(': '), f.col("DESCRIPTION")))


)
count_var(tmp, 'uniqpregid')
display(tmp)

# COMMAND ----------

# DBTITLE 1,Frequency table of 'null' codes
tmp_null = (tmp
       .filter(f.col("DESCRIPTION").isNull())
)
tab(tmp_null, 'CODE')
display(tab(tmp_null, 'CODE'))
display(tmp_null.limit(100 ))

# COMMAND ----------

# MAGIC %md
# MAGIC #3. Diagnosis and History

# COMMAND ----------

#from pyspark.sql.window import Window
#from pyspark.sql.functions import col, row_number
tmp2 = (tmp
    .groupBy("CODE")
    .agg(f.count('*').alias('recs'))
)
# display(tmp2)

w3 = Window.partitionBy().orderBy(f.col("recs").desc())

top10_codes = (tmp2
    .withColumn("rank", f.row_number().over(w3))
    .select( "CODE", "rank"  )
    .join(tmp.select( "CODE", "DATE", "uniqpregid",  "FullName") , "CODE", "right")
    .withColumn( "topNname", f.when( f.col("rank" ) < 10, f.col("FullName"))
                .otherwise(f.lit( "Other") ))
    .withColumn('_DATE', f.date_format(f.col('DATE'), 'yyyy-MM'))
    .where(f.col('_DATE').isNotNull())
    .groupBy('_DATE', 'topNname') 
        .agg(f.count('*').alias('recs'))
)

display(top10_codes)

# COMMAND ----------

# DBTITLE 1,Table of popular code
#from pyspark.sql.window import Window
#from pyspark.sql.functions import col, row_number
tmp2 = (tmp
    .groupBy("CODE")
    .agg(f.count('*').alias('recs'))
)
# display(tmp2)

w3 = Window.partitionBy().orderBy(f.col("recs").desc())

top10_codes = (tmp2
    .withColumn("rank", f.row_number().over(w3))
    .select( "CODE", "rank"  )
    .join(tmp.select( "CODE", "DATE", "uniqpregid",  "FullName") , "CODE", "right")
    .withColumn( "topNname", f.when( f.col("rank" ) < 501, f.col("FullName"))
                .otherwise(f.lit( "Other") ))
    #.withColumn('_DATE', f.date_format(f.col('DATE'), 'yyyy-MM'))
    #.where(f.col('_DATE').isNotNull())
    .groupBy('topNname') # .groupBy('topNname', 'CODE')
        .agg(f.count('*').alias('recs'))
)

display(top10_codes)

# COMMAND ----------

# DBTITLE 1,Checking the code behind 'null'
#from pyspark.sql.window import Window
#from pyspark.sql.functions import col, row_number
tmp2 = (tmp
    .groupBy("CODE")
    .agg(f.count('*').alias('recs'))
)
# display(tmp2)

w3 = Window.partitionBy().orderBy(f.col("recs").desc())

top10_codes = (tmp2
    .withColumn("rank", f.row_number().over(w3))
    .select( "CODE", "rank"  )
    .join(tmp.select( "CODE", "DATE", "uniqpregid",  "FullName") , "CODE", "right")
    .withColumn( "topNname", f.when( f.col("rank" ) < 501, f.col("FullName"))
                .otherwise(f.lit( "Other") ))
    #.withColumn('_DATE', f.date_format(f.col('DATE'), 'yyyy-MM'))
    #.where(f.col('_DATE').isNotNull())
    .groupBy('topNname', 'CODE')
        .agg(f.count('*').alias('recs'))
)

display(top10_codes)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Rounding to nearest 5

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType

# small additional code to convert from pandas dataframe to a spark dataframe (and converting to the correct col type)
#out_ssnap_categorical_freq = spark.createDataFrame(out_ssnap_categorical_freq).withColumn("freq_n", f.col("freq_n").cast(IntegerType()))

# which col or cols to apply to (ie. which cols have the counts that need disclosure control)
cols = ['recs']


for i, var in enumerate(cols):
  typ = dict(top10_codes.dtypes)[var]
  #print(i, var, typ)
  assert str(typ) in('bigint')
  assert top10_codes.where(f.col(var)<0).count() == 0
  top_10_codes_sdc = (
      top10_codes         
      .withColumn(var,
                  f.when(f.col(var) == 0, 0)
                     .when(f.col(var) < 10, 10)
                     .when(f.col(var) >= 10, 5*f.round(f.col(var)/5))
                    )
        )

# COMMAND ----------

display(top_10_codes_sdc)

# COMMAND ----------

snomed_counts = (maternity_interpreter_GDPPR 
        #.where(f.col('_diagnosisdate').isNotNull())
#        .withColumn('diagnosisandhistory', f.regexp_replace(f.col('diagnosisandhistory'), "[^\w\s]+" , ""))
        .groupBy("CODE")
            .agg(f.count('*').alias('recs'))
        .withColumn("Total", f.lit(maternity_interpreter_GDPPR.count()) )
        .withColumn("Prop",  (f.col("recs")*100)/ f.col("Total") )
        .join(snomed_ref, f.col("CODE") == f.col("conceptId") , 'left')
        .withColumn("FullName", f.concat(f.col("CODE"), f.lit(': '), f.col("term")))
#        .drop("Total", "ALT_CODE" )
        .sort("recs", ascending= False) 
      )
      
display(snomed_counts )