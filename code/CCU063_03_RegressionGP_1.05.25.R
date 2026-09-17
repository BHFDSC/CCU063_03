pacman::p_load(
  rio,          # File import
  here,         # File locator
  tidyverse,    # data management + ggplot2 graphics, 
  stringr,      # manipulate text strings 
  purrr,        # loop over objects in a tidy way
  gtsummary,    # summary statistics and tests 
  broom,        # tidy up results from regressions
  lmtest,       # likelihood-ratio tests
  parameters,   # alternative to tidy up results from regressions
  see          # alternative to visualise forest plots
)

###############################################
# This section creates a function called      #
# logisticPseudoR2s().  To use it             #
# type logisticPseudoR2s(myLogisticModel)     #
###############################################
logisticPseudoR2s <- function(LogModel) {
  dev <- LogModel$deviance 
  nullDev <- LogModel$null.deviance 
  modelN <-  length(LogModel$fitted.values)
  R.l <-  1 -  dev / nullDev
  R.cs <- 1- exp ( -(nullDev - dev) / modelN)
  R.n <- R.cs / ( 1 - ( exp (-(nullDev / modelN))))
  cat("Pseudo R^2 for logistic regression\n")
  cat("Hosmer and Lemeshow R^2  ", round(R.l, 3), "\n")
  cat("Cox and Snell R^2        ", round(R.cs, 3), "\n")
  cat("Nagelkerke R^2           ", round(R.n, 3),    "\n")
}

##Remove all people with missing data for relevant variables

mydata2 <- mydata1 %>%
  filter(!is.na(agefinal), !is.na(eth5), !is.na(imd_quintile), !is.na(region), !is.na(prev_preg))

#univariate logistic regression


univ_reg <- glm(fact_of_gp_interaction ~ interpreter_use, family = binomial, data = mydata2, na.action = na.exclude)
summary(univ_reg)



###table for excel

univ_tab_base <- univ_reg %>%
  broom::tidy(exponentiate = TRUE, conf.int = TRUE) 

logisticPseudoR2s(univ_reg)

#multivariable regression with ethnicity

mv_reg <- glm(fact_of_gp_interaction ~ interpreter_use + agefinal+ eth5 + imd_quintile + region + prev_preg, family = binomial, data = mydata2, na.action = na.exclude)
summary(mv_reg)

#####
mv_tab_base <- mv_reg %>%
  broom::tidy(exponentiate = TRUE, conf.int = TRUE) 

logisticPseudoR2s(mv_reg)

#multivariable regression without ethnicity

mv_reg1 <- glm(fact_of_gp_interaction ~ interpreter_use + agefinal + imd_quintile + region + prev_preg, family = binomial, data = mydata2, na.action = na.exclude)
summary(mv_reg1)

#####
mv_tab_base1 <- mv_reg1 %>%
  broom::tidy(exponentiate = TRUE, conf.int = TRUE) 

logisticPseudoR2s(mv_reg1)

##save mv_tab_base
write.csv(univ_tab_base, "/CCU063_03/GPuniregression.24.05.csv", row.names = FALSE)

write.csv(mv_tab_base, "/CCU063_03/GPmultiregression.24.05.csv", row.names = FALSE)

write.csv(mv_tab_base1, "/CCU063_03/GPmultiregressionwoethnicity.24.05.csv", row.names = FALSE)

