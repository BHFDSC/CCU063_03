install.packages("table1")
library(table1)

##Limit to participants with preconception period during-COVID

mydata2 <- mydata1 %>%
  filter(covid_period_applicable == "pre")

#Table without rounding#

table1 <- table1(~ agefinal + eth5 + imd_quintile + prev_preg + region + folic_acid_pre_pregnancy + ovsvischcat + complexsocialfactors + booking_after_10weeks|interpreter_use , 
                 data = mydata2)
print(table1)


# Custom rounding function to round counts greater than 10 to nearest multiple of 5

round_to_nearest_5 <- function(x) {
  if (is.na(x)) {
    return(NA)  # Return NA if the value is NA
  }
  if (x > 10) {
    return(round(x / 5) * 5)  # Round to nearest 5 if count is greater than 10
  } else {
    return(x)  # Leave counts <= 10 as they are
  }
}

#Create table with rounded counts (NOTE - MISSING AND TOTALS NOT ROUNDED SO DO THIS MANUALLY)

my.render.cat <- function(x) {    c("", sapply(stats.default(x), function(y) with(y,        sprintf("%d (%0.0f %%)", round_to_nearest_5(FREQ), PCT))))}

table1<- table1(~ agefinal + eth5 + imd_quintile + prev_preg + region + folic_acid_pre_pregnancy + ovsvischcat + complexsocialfactors + booking_after_10weeks + fact_of_gp_interaction + fact_of_ae_interaction|interpreter_use , 
                data = mydata2, render.categorical=my.render.cat)
print(table1)

#Convert table to dataframe so it can be saved as a CVS file
table = as.data.frame(table1)

#Save table
write.csv(table, "/CCU063_03/table1_preCOVID.24.05.csv", row.names = FALSE)


# Calculate median age

# Summary by interpreter use
by_group <- mydata2 %>%
  group_by(interpreter_use) %>%
  summarise(
    median_age = median(agefinal, na.rm = TRUE),
    IQR_age = IQR(agefinal, na.rm = TRUE),
    .groups = "drop"
  )

# Summary for total population
total <- mydata2 %>%
  summarise(
    interpreter_use = "Total",
    median_age = median(agefinal, na.rm = TRUE),
    IQR_age = IQR(agefinal, na.rm = TRUE)
  )

# Combine total with group summaries
final_summary <- bind_rows(by_group, total)

print(final_summary)

#Save table
write.csv(final_summary, "/CCU063_03/medianage_precovid.24.05.csv", row.names = FALSE)
