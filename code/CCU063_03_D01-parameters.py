# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # CCU063_03-D01-parameters
# MAGIC
# MAGIC # **Project** CCU063_03
# MAGIC
# MAGIC **Description** This notebook defines a set of parameters, which is loaded in each notebook in the data curation pipeline, so that helper functions and parameters are consistently available.
# MAGIC
# MAGIC **Author(s)** Majel McGranahan, Lars Murdock
# MAGIC
# MAGIC **Reviewers** ⚠ UNREVIEWED
# MAGIC
# MAGIC **Acknowledgements** Based on CCU085_01-D01-parameters by Jadene Lewis, Tom Bolton (Health Data Science Team, BHF Data Science Centre) which was based on CCU004_01-D01-parameters (Tom Bolton, Fionna Chalmers, Anna Stevenson)
# MAGIC
# MAGIC **Notes** This pipeline has an initial production date of 2024-12-20 (`pipeline_production_date` == `2024-12-20`) and the `archived_on` dates used for each dataset correspond to the latest (most recent) batch of data before this date. Should the pipeline and all the notebooks that follow need to be updated and rerun, then this notebook should be rerun directly (before being called by subsequent notebooks) with `pipeline_production_date` updated and `run_all_toggle` switched to True. After this notebook has been rerun the `run_all_toggle` should be reset to False to prevent subsequent notebooks that call this notebook from having to rerun the 'archived_on' section. Rerunning this notebook with the updated `pipeline_production_date` will ensure that the `archived_on` dates used for each dataset are updated with these dates being saved for reference in the collabortion database.
# MAGIC
# MAGIC **Versions** Version 1.1 as at '2025-05-23'
# MAGIC
# MAGIC **Data Output** 
# MAGIC **`ccu063_03_parameters_df_datasets`**: table of `archived_on` dates for each dataset that can be used consistently throughout the pipeline 

# COMMAND ----------

# MAGIC %md # 0. Setup

# COMMAND ----------

run_all_toggle = False # True

# COMMAND ----------

spark.conf.set('spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation', 'true')

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Libraries

# COMMAND ----------

import pyspark.sql.functions as f
import pyspark.sql.types as t
import pandas as pd
import re
import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Common Functions

# COMMAND ----------

# MAGIC %run "Workspace/Shared/SHDS/common/functions"

# COMMAND ----------

#From CCU085_01 demographics

import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import Window

from functools import reduce

import databricks.koalas as ks
import pandas as pd
import pyspark.pandas as ps
import numpy as np

import re
import io
import datetime

import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib import dates as mdates
import seaborn as sns

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Paths and Variables

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 Set Project Specific Variables

# COMMAND ----------

# Please set and check the variables below



# -----------------------------------------------------------------------------
# Databases
# -----------------------------------------------------------------------------
db = ''
dbc_old = f'{db}_collab'
dbc = ''
dsa = f''

# -----------------------------------------------------------------------------
# Project
# -----------------------------------------------------------------------------
proj = 'ccu063_03' 


# -----------------------------------------------------------------------------
# Dates- pregnancy start dates between these dates - May need to update these dates
# -----------------------------------------------------------------------------
study_start_date = '2019-03-01' ## 
study_end_date   = '2024-02-29' ## 

# -----------------------------------------------------------------------------
#Archived on date
# -----------------------------------------------------------------------------

tmp_archived_on = '2025-02-04'
print(f'Archived on date: {tmp_archived_on}')

# -----------------------------------------------------------------------------
#Gestation variables
# -----------------------------------------------------------------------------
min_plausible_gest = 28
max_plausible_gest = 315
min_livebirths = 0
max_livebirths = 24
min_stillbirths = 0
max_stillbirths = 24
min_earlyloss = 0
max_earlyloss = 30

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.3 Curated Data Paths

# COMMAND ----------

# -----------------------------------------------------------------------------
# These are paths to data tables curated in subsequent notebooks that may be
# needed in subsequent notebooks from which they were curated
# -----------------------------------------------------------------------------

# note: the below is largely listed in order of appearance within the pipeline:  

# reference tables
##path_ref_bhf_phenotypes  = 'bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127'
#path_ref_geog            = '.ons_chd_geo_listings'
# path_ref_imd             = '.english_indices_of_dep_v02'
path_ref_gp_refset       = '.gpdata_snomed_refset_full'
path_ref_gdppr_refset    = '.gdppr_cluster_refset'
path_ref_icd10           = '.icd10_group_chapter_v01'
#path_ref_opcs4           = '.opcs_codes_v02'
#path_ref_imd             = '.hds_cur_lsoa_2011_imd_lookup'
#path_ref_region          = '.hds_cur_lsoa_region_lookup'  
# path_ref_map_ctv3_snomed = '.read_codes_map_ctv3_to_snomed'
# path_ref_ethnic_hes      = '.hesf_ethnicity'
# path_ref_ethnic_gdppr    = '.gdppr_ethnicity'

# curated tables
#path_cur_deaths_sing       = f'{dsa}.{proj}_cur_deaths_sing'
#path_cur_deaths_long       = f'{dsa}.{proj}_cur_deaths_long'
path_cur_hes_apc_long      = f'{dsa}.{proj}_cur_hes_apc_long' 

# path_cur_hes_apc_long      = f'{dsa}.{proj}_cur_hes_apc_all_years_archive_long'
# path_cur_hes_apc_oper_long = f'{dsa}.{proj}_cur_hes_apc_all_years_archive_oper_long'
# path_cur_lsoa_region       = f'{dsa}.{proj}_cur_lsoa_region_lookup'
# path_cur_lsoa_imd          = f'{dsa}.{proj}_cur_lsoa_imd_lookup'
# path_cur_lsoa              = f'{dsa}.{proj}_lsoa'

#path_cur_lsoa_ruc          = f'{dsa}.{proj}_cur_lsoa_ruc_lookup'

path_cur_hes_apc_spells    = f'{dsa}.{proj}_cur_hes_apc_spells'

#path_cur_covid_inf         = f'{dsa}.{proj}_cur_covid_inf'
#path_cur_covid_vacc        = f'{dsa}.{proj}_cur_covid_vacc'

#path_cur_covid_vacc_first        = f'{dsa}.{proj}_cur_covid_vacc_first' 
#path_cur_covid_vacc_qa           = f'{dsa}.{proj}_cur_covid_vacc_qa'
#path_cur_covid_vacc_reshaped     = f'{dsa}.{proj}_cur_covid_vacc_reshaped'
#path_cur_lsoa_multisource        = f'{dsa}.{proj}_cur_lsoa_multisource'

# HDS curated assets: Demographics table
hds_curated_assets_demographic = f'{dsa}.hds_curated_assets__demographics_2025_02_04' # HDS team key patient characteristics table

# HDS curated assets: Multisource table
hds_curated_assets_lsoa_multisource = f'{dsa}.hds_curated_assets__lsoa_multisource_2025_02_04'

# HDS curated assets: curated deaths (single row) table
#hds_curated_assets_deaths_sing = f'{dsa}.hds_curated_assets__deaths_single_2024_07_23'

# HDS curated assets: curated cause of death (long) table
#hds_curated_assets_deaths_long = f'{dsa}.hds_curated_assets__deaths_cause_of_death_2024_07_23'

# HDS curated assets: curated HES APC table (long)
hds_curated_assets_hes_apc_long = f'{dsa}.hds_curated_assets__hes_apc_diagnosis_2025_02_04'

# HDS curated assets: curated COVID diagnoses table
#hds_curated_assets_covid_positive = f'{dsa}.hds_curated_assets__covid_positive_2024_07_23'

# Patient ID table (with flag column for valid NHS Numbers)
patient_id_table = f'.token_pseudo_id_lookup'

# # temporary tables
path_tmp_skinny_unassembled             = f'{dsa}.{proj}_tmp_kpc_harmonised_1'
path_tmp_skinny_assembled               = f'{dsa}.{proj}_tmp_kpc_selected'
path_tmp_skinny                         = f'{dsa}.{proj}_tmp_skinny'

path_tmp_quality_assurance_hx_1st_wide  = f'{dsa}.{proj}_tmp_quality_assurance_hx_1st_wide'
path_tmp_quality_assurance_hx_1st       = f'{dsa}.{proj}_tmp_quality_assurance_hx_1st'
path_tmp_quality_assurance_qax          = f'{dsa}.{proj}_tmp_quality_assurance_qax'
path_tmp_quality_assurance              = f'{dsa}.{proj}_tmp_quality_assurance'


path_tmp_demographics                   = f'{dsa}.{proj}_tmp_demographics'
path_tmp_lsoa                           = f'{dsa}.{proj}_tmp_lsoa'
path_tmp_lsoa_inc_exc                   = f'{dsa}.{proj}_tmp_lsoa_inc_exc'

# path_tmp_cur_covid_inf                   = f'{dsa}.{proj}_tmp_cur_covid_inf'

# path_tmp_cur_covid_vacc                  = f'{dsa}.{proj}_tmp_cur_covid_vacc'

path_tmp_test_cases                     = f'{dsa}.{proj}_tmp_test_cases'
path_tmp_test_controls                  = f'{dsa}.{proj}_tmp_test_controls'
path_tmp_test_controls_lsoa             = f'{dsa}.{proj}_tmp_test_controls_lsoa'
path_tmp_test_controls_covid_inf        = f'{dsa}.{proj}_tmp_test_controls_covid_inf'
path_tmp_test_controls_covid_vacc       = f'{dsa}.{proj}_tmp_test_controls_covid_vacc'

path_tmp_cases                          = f'{dsa}.{proj}_tmp_cases'
path_tmp_pcontrols                      = f'{dsa}.{proj}_tmp_pcontrols'

# path_tmp_inc_exc_cohort                 = f'{dsa}.{proj}_tmp_inc_exc_cohort'
path_tmp_inc_exc_flow                   = f'{dsa}.{proj}_tmp_inc_exc_flow'
path_tmp_inc_exc_flow_cohort            = f'{dsa}.{proj}_tmp_inc_exc_flow_cohort'

path_tmp_hx_nonfatal                    = f'{dsa}.{proj}_tmp_hx_nonfatal'

path_tmp_inc_exc_2_cohort                 = f'{dsa}.{proj}_tmp_inc_exc_2_cohort'
path_tmp_inc_exc_2_flow                   = f'{dsa}.{proj}_tmp_inc_exc_2_flow'

# path_tmp_covariates_hes_apc             = f'{dbc}.{proj}_tmp_covariates_hes_apc'
# path_tmp_covariates_pmeds               = f'{dbc}.{proj}_tmp_covariates_pmeds'
# path_tmp_covariates_lsoa                = f'{dbc}.{proj}_tmp_covariates_lsoa'
# path_tmp_covariates_lsoa_2              = f'{dbc}.{proj}_tmp_covariates_lsoa_2'
# path_tmp_covariates_lsoa_3              = f'{dbc}.{proj}_tmp_covariates_lsoa_3'
# path_tmp_covariates_n_consultations     = f'{dbc}.{proj}_tmp_covariates_n_consultations'
# path_tmp_covariates_unique_bnf_chapters = f'{dbc}.{proj}_tmp_covariates_unique_bnf_chapters'
# path_tmp_covariates_hx_out_1st_wide     = f'{dbc}.{proj}_tmp_covariates_hx_out_1st_wide'
# path_tmp_covariates_hx_com_1st_wide     = f'{dbc}.{proj}_tmp_covariates_hx_com_1st_wide'

# out tables
path_out_codelist_quality_assurance      = f'{dsa}.{proj}_out_codelist_quality_assurance'
# path_out_codelist_cvd                    = f'{dsa}.{proj}_out_codelist_cvd'
# path_out_codelist_comorbidity            = f'{dbc}.{proj}_out_codelist_comorbidity'
path_out_codelist_covid                  = f'{dsa}.{proj}_out_codelist_covid'
path_out_codelist_covariates             = f'{dsa}.{proj}_out_codelist_covariates'
path_out_codelist_stroke                 = f'{dsa}.{proj}_out_codelist_stroke'
# path_out_codelist_covariates_markers     = f'{dsa}.{proj}_out_codelist_covariates_markers'
# path_out_codelist_outcomes               = f'{dsa}.{proj}_out_codelist_outcomes'

##path_out_ssnap                             = f'{dsa}.{proj}_out_ssnap'
##path_out_ssnap_id_date                     = f'{dsa}.{proj}_out_ssnap_id_date'
##path_out_ssnap_covariates                  = f'{dsa}.{proj}_out_ssnap_covariates'

path_out_demographics                      = f'{dsa}.{proj}_out_demographics'
path_out_lsoa                              = f'{dsa}.{proj}_out_lsoa'

path_out_covid_inf                         = f'{dsa}.{proj}_out_covid_inf'
path_out_covid_vacc                        = f'{dsa}.{proj}_out_covid_vacc'

path_out_hometime                          = f'{dsa}.{proj}_out_hometime'

path_out_controls                          = f'{dsa}.{proj}_out_controls'

path_out_controls_hometime                 = f'{dsa}.{proj}_out_controls_hometime'

# path_out_covariates                 = f'{dbc}.{proj}_out_covariates'
# path_out_exposures                  = f'{dsa}.{proj}_out_exposures'
# path_out_outcomes                   = f'{dsa}.{proj}_out_outcomes'
