library(DBI)

library(odbc)

databricks_conn <- dbConnect(odbc(), "Databricks")

library(DBI)
con <- dbConnect(
  odbc::odbc(),
  dsn = 'databricks',
  HTTPPath = '',
  PWD = rstudioapi::askForPassword('Please enter Databricks PAT')
)


# install.packages -------------------------------------------------------------
install.packages(odbc)
install.packages(DBI)
install.packages(dbplyr)
install.packages(dplyr)
install.packages(lubridate)

# load required packages -------------------------------------------------------
library(odbc)
library(DBI)
library(dbplyr)
library(dplyr)
library(lubridate)

mydata1<- dbGetQuery(con,"SELECT person_id_mother_deid, uniqpregid, est_preg_start, lookback_start, covid_period_applicable, SNOMED_conceptId, SNOMED_conceptId_description, DATE_interpreter, Dob, eth5, region, imd_quintile, imd_decile, in_gdppr, gdppr_min_date, interpreter_use, record_before_lookback, agefinal, delivery_date, folicacid, ovsvischcat, complexsocialfactors, gestagebooking, booking_after_10weeks, prev_preg, previouslivebirths, previousstillbirths, previouslosseslessthan24weeks, fact_of_gp_interaction, fact_of_ae_interaction 
  FROM .ccu063_03_cohort_ae_interaction_counts2")

