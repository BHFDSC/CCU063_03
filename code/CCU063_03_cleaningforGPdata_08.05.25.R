install.packages('pacman')
install.packages('rio')
install.packages('here')
install.packages('janitor')
install.packages('lubridate')
install.packages('epikit')
install.packages('tidyverse')
install.packages('skimr')

##load relevant packages###
pacman::p_load(
  rio,        # importing data  
  here,       # relative file pathways  
  janitor,    # data cleaning and tables
  lubridate,  # working with dates
  epikit,     # age_categories() function
  tidyverse   # data management and visualization
)

library(dplyr)
library(tidyverse)
library(skimr)

install.packages("forcats")
library(forcats)

##rename variables to be lower case##
mydata1 <- mydata1 %>%
  janitor::clean_names()

##view new variable names#
names(mydata1)


#check values# ---- #Remove '#'s' to run

#summary(mydata1$gestagebooking, na.rm=T)
#summary(mydata1$previouslosseslessthan24weeks, na.rm=T)
#summary(mydata1$agefinal, na.rm=T)
#summary(mydata1$previouslosseslessthan24weeks, na.rm=T)
#summary(mydata1$previousstillbirths, na.rm=T)
#summary(mydata1$previouslivebirths, na.rm=T)


##create gestational age at booking in weeks variable ###
mydata1 <- mydata1 %>%
  mutate(gest_age_booking_wks = gestagebooking / 7,
         .before = 3)

##create gestational age at booking after 10 weeks##
#mydata1$gest_age_over_10wks<-
 # ifelse(mydata1$gest_age_booking_wks > 10, "Y", NA)
#mydata1$gest_age_over_10wks<-
 # ifelse(mydata1$gest_age_booking_wks <=10, "N", mydata1$gest_age_over_10wks)

mydata1 <- mydata1 %>%
  mutate(booking_after_10weeks = as_factor(booking_after_10weeks))

mydata1 <- mydata1 %>%
  mutate(booking_after_10weeks = fct_relevel(booking_after_10weeks, "yes", "no", ))

mydata1 <- mydata1 %>%
 mutate(booking_after_10weeks = as_factor(booking_after_10weeks))



##check character of variables## Remove '#s' to run

#class(mydata1$agefinal)
#class(mydata1$eth5)
#class(mydata1$region)
#class(mydata1$interpreter_use)
#class(mydata1$imd_quintile)
#class(mydata1$folicacid)
#class(mydata1$ovsvischcat)
#class(mydata1$complexsocialfactors)
#class(mydata1$booking_after_10weeks)
#class(mydata1$prev_preg)
#class(mydata1$fact_of_gp_interaction)
#class(mydata1$fact_of_ae_interaction)
#class(mydata1$region)
#class(mydata1$COVID_period_applicable)


#change variables from character to factor variables####

mydata1 <- mydata1 %>%
    mutate(imd_quintile = as.character(imd_quintile))

#multiple#
mydata1 <- mydata1 %>%
  mutate(across(.cols = c(eth5, 
                          region, 
                          interpreter_use, 
                          imd_quintile, 
                          folicacid, 
                          ovsvischcat, 
                          complexsocialfactors,
                          prev_preg,
                          fact_of_gp_interaction,
                          fact_of_ae_interaction,
                          covid_period_applicable), 
                .fns = as.factor))

#look at data again#
#skimr::skim(mydata1) --- no package called skimr available

#determine number of values within variable# Remove '#s' to run
#count(mydata1, eth5)
#count(mydata1, region)
#count(mydata1, interpreter_use)
#count(mydata1, imd_quintile)
#count(mydata1, folicacid)
#count(mydata1, ovsvischcat)
#count(mydata1, complexsocialfactors)
#count(mydata1, gest_age_over_10wks)
#count(mydata1, prev_preg)
#count(mydata1, fact_of_gp_interaction)
#count(mydata1, fact_of_ae_interaction)
#count(mydata1, region)
#count(mydata1, covid_period_applicable)


##create ethnic minority variable###

mydata1 <- mydata1 %>%
  mutate(ethnic_minority = recode(eth5, 
                                  "White" = "N",
                                  "Mixed or multiple ethnic groups" = "Y",
                                  "Asian or Asian British" = "Y",
                                  "Black, Black British, Caribbean or African" = "Y",
                                  "Other ethnic group" = "Y"))

#turn into factor variable#
mydata1 <- mydata1 %>%
  mutate(ethnic_minority = as.factor(ethnic_minority))

mydata1 <- mydata1 %>%
  mutate(ethnic_minority = fct_relevel(ethnic_minority, "Y", "N"))

#Rename COVID periods


mydata1 <- mydata1 %>%
  mutate(covid_period_applicable = recode(covid_period_applicable, 
                                  "Conception_before_Covid_start" = "pre",
                                  "Lookback_period_after_Covid_start" = "post",
                                  "Lookback_period_spans_Covid_start" = "during"))


##revel GP attendance
mydata1 <- mydata1 %>%
  mutate(fact_of_gp_interaction = fct_relevel(fact_of_gp_interaction, "No_interactions", "One_or_more"))

##revel A+E attendance
mydata1 <- mydata1 %>%
  mutate(fact_of_ae_interaction = fct_relevel(fact_of_ae_interaction, "No_interactions", "One_or_more"))

#remove blank row in ethnic minority
mydata1$ethnic_minority <- 
  factor(mydata1$ethnic_minority)

#check
count(mydata1, ethnic_minority)

##Switch IMD quintile from factor to ordinal factor variable####
mydata1 <- mydata1 %>%
  mutate(imd_quintile = fct_relevel(imd_quintile, "1", "2", "3", "4", "5"))

levels(mydata1$imd_quintile)


##convert other categorical to ordered factor variables##

mydata1 <- mydata1 %>%
  mutate(complexsocialfactors = fct_relevel(complexsocialfactors, "Y", "N"))

mydata1 <- mydata1 %>%
  mutate(ovsvischcat = fct_relevel(ovsvischcat, "A", "B", "C", "D", "E", "F", "P"))

mydata1 <- mydata1 %>%
  mutate(folicacid = fct_relevel(folicacid, "01", "02", "03"))

mydata1 <- mydata1 %>%
  mutate(eth5 = fct_relevel(eth5, "White", "Black, Black British, Caribbean or African", "Asian or Asian British", "Mixed or multiple ethnic groups", "Other ethnic group"))

mydata1 <- mydata1 %>%
  mutate(region = fct_relevel(region, "London", "South East", "South West", "East of England", "East Midlands", "West Midlands", "Yorkshire and The Humber", "North East", "North West"))



###Create folic acid before pregnancy variable but with NA### 
###source: https://bookdown.org/ejvanholm/WorkingWithData/creating-variables.html####

##alternative approach:
#####bookings_table_new <- bookings_table_new %>%
####mutate(folic_acid_pre_pregnancy = recode(folic_acid_supplement,
####    "01" = "Y",
####"02" = "N",
#### "03" = "N"))####


mydata1$folic_acid_pre_pregnancy <- ifelse(mydata1$folicacid== "01", "Y", NA)
mydata1$folic_acid_pre_pregnancy <- ifelse(mydata1$folicacid=="02"|
                                                        mydata1$folicacid=="03", "N", mydata1$folic_acid_pre_pregnancy)

###turn folic_acid_pre_pregnancy into factor variable###
mydata1 <- mydata1 %>%
  mutate(folic_acid_pre_pregnancy = as_factor(folic_acid_pre_pregnancy))

mydata1 <- mydata1 %>%
  mutate(folic_acid_pre_pregnancy = fct_relevel(folic_acid_pre_pregnancy, "Y", "N"))

