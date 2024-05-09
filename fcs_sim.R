#install.packages("readr")
#install.packages("dplyr")
#install.packages("lubridate")
#install.packages("tseries")
#install.packages("forecast")
#install.packages("vars")
#install.packages("urca")
#install.packages("tsDyn")
#install.packages("tidyverse")
#install.packages("tempdisagg")
#install.packages("xts")
#install.packages("tsbox")
#install.packages("MTS")
#install.packages("BVAR")
#install.packages("psych")

getwd()
#setwd("./VSCode/master-thesis")
#setwd("./master-thesis")
library(readr)  # For reading CSV files
library(dplyr)  # For data manipulation
library(lubridate)  # For date parsing
library(tseries)
library(vars)
library(forecast)
library(urca)
library(tsDyn)
library(tidyverse)
library(tempdisagg)
library(xts)
library(tsbox)
library(data.table)
library(progress)
library(MTS)
library(BVAR)
library(psych)


# STEP 1: READ CSV



spot <- read_delim('./data/spot/clarkson_data.csv', 
                   delim = ';', 
                   escape_double = FALSE, 
                   col_types = cols(Date = col_date(format = "%d/%m/%Y")),
                   trim_ws = TRUE)


pmx_forw <- read_delim('./data/ffa/PMAX_FFA.csv', 
                       delim = ';', 
                       escape_double = FALSE, 
                       col_types = cols(Date = col_date(format = "%d.%m.%Y")),
                       trim_ws = TRUE)

csz_forw <- read_delim('./data/ffa/CSZ_FFA.csv', 
                       delim = ';', 
                       escape_double = FALSE, 
                       col_types = cols(Date = col_date(format = "%d.%m.%Y")),
                       trim_ws = TRUE)

smx_forw <- read_delim('./data/ffa/SMX_FFA.csv', 
                       delim = ';', 
                       escape_double = FALSE, 
                       col_types = cols(Date = col_date(format = "%d.%m.%Y")),
                       trim_ws = TRUE)

# Macro data is reading nicely now
gbti_dev <- read_csv('./data/other/gbti_daily.csv',
                     col_types = cols(Date = col_date(format = "%d-%m-%Y")),
                     trim_ws = TRUE)

oecd_ip_dev <- read_csv('./data/other/oecd_daily.csv',
                        col_types = cols(Date = col_date(format = "%d-%m-%Y")),
                        trim_ws = TRUE)


fleet_age <- read_csv('./data/other/fleet_age_daily.csv',
                      col_types = cols(Date = col_date(format = "%d-%m-%Y")),
                      trim_ws = TRUE)


fleet_dev <- read_csv('./data/other/fleet_dev_daily.csv',
                      col_types = cols(Date = col_date(format = "%d-%m-%Y")),
                      trim_ws = TRUE)

orderbook <- read_csv('./data/other/orderbook_daily.csv',
                      col_types = cols(Date = col_date(format = "%d-%m-%Y")),
                      trim_ws = TRUE)

steel_prod <- read_csv('./data/other/steel_prod_daily.csv',
                       col_types = cols(Date = col_date(format = "%d-%m-%Y")),
                       trim_ws = TRUE)

vessel_sale_volume <- read_csv('./data/other/vessel_sale_daily.csv',
                               col_types = cols(Date = col_date(format = "%d-%m-%Y")),
                               trim_ws = TRUE)

eur_usd <- read_delim('./data/other/EUR_USD_historical.csv', 
                      delim = ';', 
                      escape_double = FALSE, 
                      col_types = cols(Date = col_date(format = "%d.%m.%Y")),
                      trim_ws = TRUE)
# Convert columns from text to numeric, replacing commas with dots
eur_usd <- eur_usd %>%
  mutate(across(-Date, ~as.numeric(gsub(",", ".", .x))))

# STEP 2: CLEAN AND PREPARE DATA
# CSZ column: CSZ
# PMX column: PMX
# SMX column: SMX
# ffa column: 1MON, 1Q


# Merge data frames on the Date column, include trade volume
#data_combined <- merge(spot, csz_forw, by = "Date")
#data_combined <- merge(data_combined, gbti_dev, by = "Date")

####### ENDRE ######
data_combined <- inner_join(spot[, c("Date", "CSZ")], csz_forw[, c("Date", "1MON")], by = "Date")
####### ENDRE ######



#data_combined <- inner_join(data_combined, gbti_dev[, c("Date", "Iron Ore Trade Vol", "Coal Trade Vol", "Grain Trade Vol", "Minor Bulk Trade Vol", "Dry Bulk Trade Vol")], by = "Date")
#data_combined <- inner_join(data_combined, oecd_ip_dev[, c("Date", "Ind Prod Excl Const VOLA")], by = "Date")
#data_combined <- inner_join(data_combined, fleet_dev[, c("Date", "HSZ fleet", "HMX fleet", "PMX fleet", "CSZ fleet")], by = "Date")
#data_combined <- inner_join(data_combined, fleet_dev[, c("Date", "PMX fleet")], by = "Date")
data_combined <- inner_join(data_combined, eur_usd[, c("Date", "Last")], by = "Date")
# Removing rows where ColumnA or ColumnB have 0 or NA values
data_combined <- data_combined %>%
  filter(if_all(-Date, ~ .x != 0 & !is.na(.x)))


# Remove rows with NA or 0 in either the S6TC_S10TC column or the 4TC_ROLL column
#data_combined <- subset(data_combined, !(is.na(CSZ) | CSZ == 0) & !(is.na(`CURMON`) | `CURMON` == 0))


# Transform data to log levels and create a new data frame for log levels
data_log_levels <- data.frame(
  Date = data_combined$Date,
  
  
  
  ####### ENDRE ######
  spot = log(data_combined$CSZ),
  forwp = log(data_combined$`1MON`)
  ####### ENDRE ######
  
)

#exog_log_levels <- data.frame(
#  Date = data_combined$Date,
#  iron = log(data_combined$`Iron Ore Trade Vol`),  # Assuming you meant to log-transform these as well
#  coal = log(data_combined$`Coal Trade Vol`),
#  grain = log(data_combined$`Grain Trade Vol`),
#  minor_bulk = log(data_combined$`Minor Bulk Trade Vol`),
#  dry_bulk = log(data_combined$`Dry Bulk Trade Vol`),
#  eur_usd = data_combined$Last  # Not log-transformed as it's a rate, but adjust according to your needs
#)

exog_log_levels <- data.frame(
  Date = data_combined$Date,
  #ind_prod = log(data_combined$`Ind Prod Excl Const VOLA`),  # Assuming you meant to log-transform these as well
  #hsz_dev = log(data_combined$`HSZ fleet`),
  #hmx_dev = log(data_combined$`HMX fleet`),
  #pmx_dev = log(data_combined$`PMX fleet`),
  #csz_dev = log(data_combined$`PMX fleet`),
  eur_usd = data_combined$Last  # Not log-transformed as it's a rate, but adjust according to your needs,
  #iron = log(data_combined$`Iron Ore Trade Vol`),
  #coal = log(data_combined$`Coal Trade Vol`)
)

# Display the first few rows of each new data frame to verify
print(head(data_log_levels))

# Split into train and test sets

# Calculate the index for the split
split_index <- round(nrow(data_combined) * 0.8)

# Split the data into training and test sets
train_lev <- data_log_levels[1:split_index, ]
test_lev <- data_log_levels[(split_index+1): nrow(data_log_levels), ]
len_test <- nrow(test_lev)

exog_lev <- exog_log_levels[1:split_index, ]

# Calculate differences

# Calculate first differences for the training set
train_diff <- data.frame(
  Date = train_lev$Date[-1],  # Exclude the first date because there's no prior value to difference with
  spot = diff(train_lev$spot),
  forwp = diff(train_lev$forwp)
)


exog_diff <- data.frame(
  Date = exog_lev$Date[-1],  # Exclude the first date
  #ind_prod = diff(exog_lev$ind_prod),
  #hsz_dev = diff(exog_log_levels$hsz_dev),
  #hmx_dev = diff(exog_log_levels$hmx_dev),
  #pmx_dev = diff(exog_log_levels$pmx_dev),
  #csz_dev = diff(exog_lev$csz_dev),
  eur_usd = diff(exog_lev$eur_usd)
  #iron = diff(exog_lev$iron),
  #coal = diff(exog_lev$coal)
)



# Print the first few rows to verify
print(head(train_diff))
print(head(exog_diff))




# Prepare time series objects

train_lev_ts <- ts(train_lev[, -1])
train_diff_ts <- ts(train_diff[, -1])

#test_log_levels_ts <- ts(test_log_levels[, -1])


exog_lev_df <- exog_lev[, -1, drop = FALSE]
exog_lev_ts <- ts(data = exog_lev_df)

exog_diff_df <- exog_diff[, -1, drop = FALSE]
exog_diff_ts <- ts(data = exog_diff_df)




# Step 3: STATIONARITY CHECKS

# Perform Augmented Dickey-Fuller Test
lapply(train_lev_ts, function(series) adf.test(series, alternative = "stationary"))
lapply(train_diff_ts, function(series) adf.test(series, alternative = "stationary"))

lapply(exog_lev_ts, function(series) adf.test(series, alternative = "stationary"))
lapply(exog_diff_ts, function(series) adf.test(series, alternative = "stationary"))

# Step 3a: OTHER STATISTICAL TESTS AND DESCRIPTIVE STATISTICS ON THE INPUT TIME SERIES

# Perform Phillips-Perron (PP) test (stationarity)
lapply(train_lev_ts, function(series) pp.test(series))
lapply(train_diff_ts, function(series) pp.test(series))

# Perform KPSS test (stationarity around trend)
lapply(train_lev_ts, function(series) kpss.test(series))
lapply(train_diff_ts, function(series) kpss.test(series))

# Perform Ljung-Box test (autocorrelation)
lapply(train_lev_ts, function(series) Box.test(series, type = "Ljung-Box"))
lapply(train_diff_ts, function(series) Box.test(series, type = "Ljung-Box"))

# Perform the Jarque-Bera test (normality)
lapply(train_lev_ts, function(series) jarque.bera.test(series))
lapply(train_diff_ts, function(series) jarque.bera.test(series))

# Retrieve the descriptive statistics
describe(data_combined[, c(2,3)])


# Step 4: MODEL FITS AND DIAGNOSTIC CHECKS

# VAR
# Select an appropriate lag order, p. You can use the VARselect function for guidance

# lev
lags <- VARselect(train_lev_ts, type = "const")
lag_order <- VARselect(train_lev_ts, type = "both")$selection["AIC(n)"]
var_model <- vars::VAR(train_lev_ts, p = lag_order)


# diff
#lags <- VARselect(train_diff_ts, type = "const")
#lag_order <- VARselect(train_diff_ts, type = "both")$selection["AIC(n)"]
#var_model <- VAR(train_diff_ts, p = lag_order, type = "both")

lags
summary(var_model)

# Extract the residuals from the VAR model
residuals_var <- residuals(var_model)

# Get the number of equations (variables) in the VAR model
num_equations <- ncol(residuals_var)

arch_var <- arch.test(var_model)
arch_var

#stab_var <- stability(var_model, type = "OLS-CUSUM")
#plot(stab_var)

# Loop through each equation and perform the Ljung-Box test on its residuals
for(i in 1:num_equations) {
  # Extract residuals for the ith equation
  res_i <- residuals_var[, i]
  
  # Create a time series object from residuals for White test
  ts_res_i <- ts(res_i)
  
  # Fit a linear model on residuals as dependent variable for White test
  lm_res_i <- lm(ts_res_i ~ 1)
  
  # Perform the Ljung-Box test, e.g., at lag 10
  lb_test <- Box.test(res_i, lag = 10, type = "Ljung-Box")
  
  # Perform the White test for heteroskedasticity
  white_test <- bptest(lm_res_i, ~ fitted(lm_res_i) + I(fitted(lm_res_i)^2), data = data.frame(ts_res_i))
  
  # Print the Ljung-Box test results
  cat("Ljung-Box test for autocorrelation in equation", i, " :\n")
  print(lb_test)
  cat("\n") # Add a newline for better readability
  
  # Print the White test results
  cat("White test for heteroskedasticity in equation", i, ":\n")
  print(white_test)
  cat("\n\n") # Add extra newlines for better readability
}

# Granger causality
spot_ts <- train_diff_ts[, 1]
forwc_ts <- train_diff_ts[, 2]
#forw1_ts <- train_diff_ts[, 3]
grangerSpot <- causality(var_model, cause = "spot")
grangerSpot
grangerForw <- causality(var_model, cause = "forwp")
grangerForw
#grangerForw2 <- causality(var_model, cause = "forw1m")
#grangerForw2

# 1. Shock from "spot" to "spot"
#var_irf_spot_to_spot <- vars::irf(var_model, impulse = "spot", response = "spot")
#plot(var_irf_spot_to_spot, main = "Shock from 'Spot' to 'Spot'")

# 2. Shock from "spot" to "forwp"
#var_irf_spot_to_forwp <- vars::irf(var_model, impulse = "spot", response = "forwp")
#plot(var_irf_spot_to_forwp, main = "Shock from 'Spot' to 'Forward'")

# 3. Shock from "spot" to "forw1m"
#var_irf_spot_to_forw1m <- irf(var_model, impulse = "spot", response = "forw1m")
#plot(var_irf_spot_to_forw1m, main = "Shock from 'spot' to 'forw1m'")


# 4. Shock from "forwp" to "forwp"
#var_irf_forwp_to_forwp <- vars::irf(var_model, impulse = "forwp", response = "forwp")
#plot(var_irf_forwp_to_forwp, main = "Shock from 'Forward' to 'Forward'")

# 4. Shock from "forwp" to "spot"
#var_irf_forwp_to_spot <- vars::irf(var_model, impulse = "forwp", response = "spot")
#plot(var_irf_forwp_to_spot, main = "Shock from 'Forward' to 'Spot'")


# 5. Shock from "forwc" to "forwc"
#var_irf_forwc_to_forwc <- irf(var_model, impulse = "forwc", response = "forwc")
#plot(var_irf_forwc_to_forwc, main = "Shock from 'forwc' to 'forwc'")

# 6. Shock from "forwc" to "forw1m"
#var_irf_forwc_to_forw1m <- irf(var_model, impulse = "forwc", response = "forw1m")
#plot(var_irf_forwc_to_forw1m, main = "Shock from 'forwc' to 'forw1m'")

# 7. Shock from "forw1m" to "spot"
#var_irf_forw1m_to_spot <- irf(var_model, impulse = "forw1m", response = "spot")
#plot(var_irf_forw1m_to_spot, main = "Shock from 'forw1m' to 'spot'")

# 8. Shock from "forw1m" to "forwc"
#var_irf_forw1m_to_forwc <- irf(var_model, impulse = "forw1m", response = "forwc")
#plot(var_irf_forw1m_to_forwc, main = "Shock from 'forw1m' to 'forwc'")

# 9. Shock from "forw1m" to "forw1m"
#var_irf_forw1m_to_forw1m <- irf(var_model, impulse = "forw1m", response = "forw1m")
#plot(var_irf_forw1m_to_forw1m, main = "Shock from 'forw1m' to 'forw1m'")

# Variance decomposition

#var_vd1 <- vars::fevd(var_model)
#plot(var_vd1)


# BAYESIAN VAR

# Selecting an appropriate lag order using the VARselect
#lags_bvar <- VARselect(train_diff_ts, type = "const")
##lags_bvar
#lag_bvar_order <- VARselect(train_diff_ts, type = "both")$selection["AIC(n)"]
#bvar_model <- bvar(train_diff_ts, lags = lag_bvar_order, type = "both")
#summary(bvar_model)
#bvar_fcs <- predict(bvar_model, horizon = 20)
#bvar_fcs
#summary(bvar_fcs) # This returns two forecasts lists (spot and forward) with different confidence bands
#bvar_fcs$fcast    # This is just a setting used in the forecasting above

# VARX

# lev
#lags <- VARselect(train_lev_ts, type = "const")
#lag_order <- VARselect(train_lev_ts, type = "both")$selection["AIC(n)"]
#varx_model <- vars::VAR(train_lev_ts, p = lag_order, type = "both", exogen = exog_lev_ts)

#summary(varx_model)

#varx_model3 <- VARX(train_lev_ts, x = exog_lev_ts , p = lag_order)

# diff
#lags <- VARselect(train_diff_ts, type = "const")
#lag_order <- VARselect(train_diff_ts, type = "both")$selection["AIC(n)"]
#varx_model <- vars::VAR(train_diff_ts, p = lag_order, type = "both", exogen = exog_diff_ts)

#summary(varx_model)

#varx_model3 <- VARX(train_diff_ts, x = exog_diff_ts , p = lag_order)


# VECM
#coint_test <- ca.jo(train_diff_ts, spec = "transitory", type = "eigen", ecdet = "const", K = lag_order)
coint_test <- ca.jo(train_lev_ts, spec = "longrun", type = "eigen", ecdet = "const", K = lag_order)
summary(coint_test)

vecm_cajo <- cajorls(coint_test, r=1)  # 'r' is the cointegration rank from the Johansen test results
summary(vecm_cajo$rlm)
#vecm_model <- VECM(train_diff_ts, lag=lag_order, r=1, include="const")
#summary(vecm_model)
# Impulse response analysis
#irf.vecm <- irf(vecm_model, n.ahead=10, boot=TRUE)

# Plot the impulse responses
#plot(irf.vecm)

vecm_var <- vec2var(coint_test, r = 1)

# Extract the residuals from the VAR model
residuals_var <- residuals(vecm_var)

# Get the number of equations (variables) in the VAR model
num_equations <- ncol(residuals_var)

arch_var <- arch.test(vecm_var)
arch_var

#stab_var <- stability(vecm_var, type = "OLS-CUSUM")
#plot(stab_var)

# Loop through each equation and perform the Ljung-Box test on its residuals
for(i in 1:num_equations) {
  # Extract residuals for the ith equation
  res_i <- residuals_var[, i]
  
  # Create a time series object from residuals for White test
  ts_res_i <- ts(res_i)
  
  # Fit a linear model on residuals as dependent variable for White test
  lm_res_i <- lm(ts_res_i ~ 1)
  
  # Perform the Ljung-Box test, e.g., at lag 10
  lb_test <- Box.test(res_i, lag = 10, type = "Ljung-Box")
  
  # Perform the White test for heteroskedasticity
  white_test <- bptest(lm_res_i, ~ fitted(lm_res_i) + I(fitted(lm_res_i)^2), data = data.frame(ts_res_i))
  
  # Print the Ljung-Box test results
  cat("Ljung-Box test for autocorrelation in equation", i, ":\n")
  print(lb_test)
  cat("\n") # Add a newline for better readability
  
  # Print the White test results
  cat("White test for heteroskedasticity in equation", i, ":\n")
  print(white_test)
  cat("\n\n") # Add extra newlines for better readability
}





# ARIMA
arima_model_spot <- auto.arima(train_lev$spot)
arima_model_forwp <- auto.arima(train_lev$forwp)

arima_res_spot <- residuals(arima_model_spot)
arima_res_forw <- residuals(arima_model_forwp)

Box.test(arima_res_spot, type = "Ljung-Box")
Box.test(arima_res_forw, type = "Ljung-Box")


# Fit a linear model to the residuals of the ARIMA model for log_Spot
lm_res_spot <- lm(arima_res_spot ~ I(1:nrow(as.data.frame(arima_res_spot))))

# Conduct the White test for heteroskedasticity on the fitted linear model
white_test_spot <- bptest(lm_res_spot)

# Print the results for log_Spot
print("White test for heteroskedasticity in ARIMA model residuals (log_Spot):")
print(white_test_spot)

# Fit a linear model to the residuals of the ARIMA model for log_4TC_FORWARD
lm_res_forw <- lm(arima_res_forw ~ I(1:nrow(as.data.frame(arima_res_forw))))

# Conduct the White test for heteroskedasticity
white_test_forw <- bptest(lm_res_forw)

# Print the results for log_4TC_FORWARD
print("White test for heteroskedasticity in ARIMA model residuals (log_4TC_FORWARD):")
print(white_test_forw)






# Step 9: Forecast future values

####### ENDRE ##########
forecast_horizon <- 10
####### ENDRE ##########



#varx


last_observation_matrix <- as.matrix(tail(exog_lev[-1], 1))

# Forecast 1 step ahead using the last known values of the exogenous variables
#varx_fcs <- VARXpred(varx_model3, newxt = last_observation_matrix, hstep = 1, orig = 0)

#vecm_forecasts <- predict(vecm_model, n.ahead = forecast_horizon)
var_fcs <- predict(var_model, n.ahead = forecast_horizon) 
vecm_var <- vec2var(coint_test)
vecm_fcs <- predict(vecm_var, n.ahead = forecast_horizon)

#n_forecast <- nrow(test_log_levels)  # Number of points to forecast equals the size of the test set

arima_fcs_spot <- forecast(arima_model_spot, h = forecast_horizon)
arima_fcs_forwp <- forecast(arima_model_forwp, h = forecast_horizon)

# Determine the number of rounds based on the test set size and forecast horizon
#num_rounds <- min(floor(len_test / forecast_horizon), 30)
num_rounds <- floor(nrow(test_lev) / forecast_horizon)

print(num_rounds)


# Initialize lists to store MSE results for each model
rmse_results <- list(
  VAR_spot = numeric(),
  VAR_forwp = numeric(),
  ARIMA_spot = numeric(),
  ARIMA_forwp = numeric(),
  RW_spot = numeric(),
  RW_forwp = numeric(),
  VECM_spot = numeric(),
  VECM_forwp = numeric(),
  VARX_spot = numeric(),
  VARX_forwp = numeric(),
  BVAR_spot = numeric(),
  BVAR_forwp = numeric()
)

mae_results <- list(
  VAR_spot = numeric(),
  VAR_forwp = numeric(),
  ARIMA_spot = numeric(),
  ARIMA_forwp = numeric(),
  RW_spot = numeric(),
  RW_forwp = numeric(),
  VECM_spot = numeric(),
  VECM_forwp = numeric(),
  VARX_spot = numeric(),
  VARX_forwp = numeric(),
  BVAR_spot = numeric(),
  BVAR_forwp = numeric()
)

mape_results <- list(
  VAR_spot = numeric(),
  VAR_forwp = numeric(),
  ARIMA_spot = numeric(),
  ARIMA_forwp = numeric(),
  RW_spot = numeric(),
  RW_forwp = numeric(),
  VECM_spot = numeric(),
  VECM_forwp = numeric(),
  VARX_spot = numeric(),
  VARX_forwp = numeric(),
  BVAR_spot = numeric(),
  BVAR_forwp = numeric()
)

# Preparing a list for forecasted and actual values for ease of access
forecasted_values <- list()
actual_values_list <- list()

direction_accuracy_results <- list(
  VAR_spot = numeric(),
  VAR_forwp = numeric(),
  ARIMA_spot = numeric(),
  ARIMA_forwp = numeric(),
  RW_spot = numeric(),
  RW_forwp = numeric(),
  VECM_spot = numeric(),
  VECM_forwp = numeric(),
  VARX_spot = numeric(),
  VARX_forwp = numeric(),
  BVAR_spot = numeric(),
  BVAR_forwp = numeric()
)

theil_results <- list(
  VAR_spot = numeric(),
  VAR_forwp = numeric(),
  ARIMA_spot = numeric(),
  ARIMA_forwp = numeric(),
  RW_spot = numeric(),
  RW_forwp = numeric(),
  VECM_spot = numeric(),
  VECM_forwp = numeric(),
  VARX_spot = numeric(),
  VARX_forwp = numeric(),
  BVAR_spot = numeric(),
  BVAR_forwp = numeric()
)

calculate_theils_u <- function(actual, forecast) {
  # Ensure the input vectors are of the same length
  if(length(actual) != length(forecast)) {
    stop("Actual and forecast vectors must be of the same length")
  }
  
  # Calculate the numerator (Root Mean Squared Error, RMSE)
  rmse <- sqrt(mean((actual - forecast)^2))
  
  # Calculate the denominator components
  rmse_actual <- sqrt(mean(actual^2))
  rmse_forecast <- sqrt(mean(forecast^2))
  
  # Calculate Theil's U
  theils_u <- rmse / (rmse_actual + rmse_forecast)
  
  return(theils_u)
}

calculate_mape <- function(actual, forecast) {
  # Ensure no zero values in actual to avoid division by zero
  if(any(actual == 0)) return(0)
  
  mape <- mean(abs(actual - forecast) / actual) * 100
  return(mape)
}



# Loop through each forecasting round
for (round in 1:num_rounds) {
  cat("Round", round)
  # Update the end index of the training set for this round
  if (round == 1){
    split_index <- split_index
  } else{
    split_index <- split_index + forecast_horizon
    
  }
  train_lev <- data_log_levels[1:split_index, ]
  test_lev <- data_log_levels[(split_index + 1):(split_index + forecast_horizon), ]
  
  
  # Recalculate differences for the updated training set
  train_diff <- data.frame(
    Date = train_lev$Date[-1],  # Exclude the first date because there's no prior value to difference with
    spot = diff(train_lev$spot),
    forwp = diff(train_lev$forwp)
  )
  
  # Re-fit models with the updated training set
  train_lev_ts <- ts(train_lev[, -1])
  train_diff_ts <- ts(train_diff[, -1])
  
  
  exog_lev <- exog_log_levels[1:split_index, ]
  exog_diff <- data.frame(
    Date = exog_lev$Date[-1],  # Exclude the first date
    #ind_prod = diff(exog_lev$ind_prod),
    #hsz_dev = diff(exog_log_levels$hsz_dev),
    #hmx_dev = diff(exog_log_levels$hmx_dev),
    #pmx_dev = diff(exog_log_levels$pmx_dev),
    #csz_dev = diff(exog_lev$csz_dev),
    eur_usd = diff(exog_lev$eur_usd)
    #iron = diff(exog_lev$iron),
    #coal = diff(exog_lev$coal)
  )
  
  exog_lev_df <- exog_lev[, -1, drop = FALSE]
  exog_lev_ts <- ts(data = exog_lev_df)
  
  exog_diff_df <- exog_diff[, -1, drop = FALSE]
  exog_diff_ts <- ts(data = exog_diff_df)
  
  
  ## VAR
  lag_order <- VARselect(train_diff_ts, type = "both")$selection["AIC(n)"]
  var_model <- vars::VAR(train_diff_ts, p = lag_order, type = "both")
  var_fcs <- predict(var_model, n.ahead = forecast_horizon)  
  
  ## VARX
  #varx_model <- VARX(train_diff_ts, x = exog_diff_ts , p = lag_order, output = FALSE)
  #last_observation_matrix <- as.matrix(tail(exog_diff[-1], 1))
  # Forecast 1 step ahead using the last known values of the exogenous variables
  #varx_fcs <- VARXpred(varx_model, newxt = last_observation_matrix, hstep = 1, orig = 0)
  
  ## BVAR
  #bvar_model <- bvar(train_diff_ts, lags = lag_order, type = "both")
  #bvar_fcs <- predict(bvar_model, horizon = forecast_horizon)
  #bvar_fcs_fcast <- summary(bvar_fcs)
  
  
  # VECM
  coint_test <- ca.jo(train_lev_ts, spec = "longrun", type = "trace", ecdet = "trend", K = lag_order)
  vecm_model <- vec2var(coint_test)
  #vecm_model <- VECM(train_diff_ts, lag=lag_order, r=1, include="none")
  vecm_fcs <- predict(vecm_model, n.ahead = forecast_horizon)
  
  
  
  ## ARIMA
  arima_model_spot <- auto.arima(train_lev$spot)
  arima_model_forwp <- auto.arima(train_lev$forwp)
  
  arima_fcs_spot <- forecast(arima_model_spot, h = forecast_horizon)
  arima_fcs_forwp <- forecast(arima_model_forwp, h = forecast_horizon)
  
  
  # Random Walk (ARIMA(0,1,0))
  rw_model_spot <- Arima(train_lev$spot, order = c(0, 1, 0))
  rw_model_forwp <- Arima(train_lev$forwp, order = c(0, 1, 0))
  
  rw_fcs_spot <- forecast(rw_model_spot, h = forecast_horizon)
  rw_fcs_forwp <- forecast(rw_model_forwp, h = forecast_horizon)
  
  
  # Step 10: Convert differenced forecasts back to levels
  
  # Last log levels from the training set
  last_spot <- tail(train_lev$spot, 1)
  last_forwp <- tail(train_lev$forwp, 1)
  
  last_act_spot <- tail(test_lev$spot, 1)
  last_act_forwp <- tail(test_lev$forwp, 1)
  
  # VAR
  var_rev_fcs_spot <- last_spot + cumsum(var_fcs$fcst[[1]][, "fcst"])
  var_rev_fcs_forwp <- last_forwp + cumsum(var_fcs$fcst[[2]][, "fcst"])
  
  #VARX
  #varx_rev_fcs_spot <- last_spot + cumsum(varx_fcs$pred["spot"])
  #varx_rev_fcs_forwp <- last_forwp + cumsum(varx_fcs$pred["forwp"])
  
  #BVAR
  #bvar_rev_fcs_spot <- last_spot + cumsum(t(bvar_fcs_fcast$quants[,,1])[,"50%"])
  #bvar_rev_fcs_forwp <- last_forwp + cumsum(t(bvar_fcs_fcast$quants[,,2])[,"50%"])
  
  # VECM
  #vecm_rev_fcs_spot <- last_spot + cumsum(vecm_fcs[, 1])
  #vecm_rev_fcs_forwp <- last_forwp + cumsum(vecm_fcs[, 2])
  #vecm_rev_fcs_spot <- last_spot + cumsum(vecm_fcs$fcst[[1]][, "fcst"])
  #vecm_rev_fcs_forwp <- last_forwp + cumsum(vecm_fcs$fcst[[2]][, "fcst"])
  
  
  act_spot <- test_lev$spot
  act_forwp <- test_lev$forwp
  
  # Store forecasted values for this round
  forecasted_values[[round]] <- list(
    VAR = list(spot = var_rev_fcs_spot, forwp = var_rev_fcs_forwp),
    VECM = list(spot = vecm_fcs$fcst[[1]][, "fcst"], forwp = vecm_fcs$fcst[[2]][, "fcst"]),
    ARIMA = list(spot = arima_fcs_spot$mean, forwp = arima_fcs_forwp$mean),
    RW = list(spot = rw_fcs_spot$mean, forwp = rw_fcs_forwp$mean)
    #VARX = list(spot =  varx_rev_fcs_spot, forwp = varx_rev_fcs_forwp)
    #BVAR = list(spot =  bvar_rev_fcs_spot, forwp = bvar_rev_fcs_forwp)
  )
  
  # Capture actual values for this round
  actual_values_list[[round]] <- list(
    spot = test_lev$spot,
    forwp = test_lev$forwp
  )
  
  # Calculate the actual direction of change
  act_dir_change_spot <- sign(last_act_spot - last_spot)
  act_dir_change_forwp <- sign(last_act_forwp - last_forwp)
  
  # For each model, calculate and store the direction accuracy
  for (model in c("VAR", "ARIMA", "RW", "VECM")) {
    # Calculate the actual direction of change for spot and forwp
    act_dir_change_spot <- sign(last_act_spot - last_spot)
    act_dir_change_forwp <- sign(last_act_forwp - last_forwp)
    
    # Predicted direction change for spot
    pred_dir_change_spot <- sign(forecasted_values[[round]][[model]]$spot[1] - last_spot)
    # Predicted direction change for forwp
    pred_dir_change_forwp <- sign(forecasted_values[[round]][[model]]$forwp[1] - last_forwp)
    
    # Determine if the model's forecasted direction matches the actual direction
    # For spot
    correct_dir_spot <- ifelse(pred_dir_change_spot == act_dir_change_spot, 1, 0)
    # For forwp
    correct_dir_forwp <- ifelse(pred_dir_change_forwp == act_dir_change_forwp, 1, 0)
    
    # Store the results
    direction_accuracy_results[[paste(model, "_spot", sep = "")]] <- c(direction_accuracy_results[[paste(model, "_spot", sep = "")]], correct_dir_spot)
    direction_accuracy_results[[paste(model, "_forwp", sep = "")]] <- c(direction_accuracy_results[[paste(model, "_forwp", sep = "")]], correct_dir_forwp)
  }
  
  
  
  
  # Calculate and append MSE for VAR forecasts
  rmse_results$VAR_spot <- c(rmse_results$VAR_spot, sqrt(mean((var_rev_fcs_spot - act_spot)^2)))
  rmse_results$VAR_forwp <- c(rmse_results$VAR_forwp, sqrt(mean((var_rev_fcs_forwp - act_forwp)^2)))
  
  # Calculate and append MSE for VARX forecasts
  #rmse_results$VARX_spot <- c(rmse_results$VARX_spot, sqrt(mean((varx_rev_fcs_spot - act_spot)^2)))
  #rmse_results$VARX_forwp <- c(rmse_results$VARX_forwp, sqrt(mean((varx_rev_fcs_forwp - act_forwp)^2)))
  
  # Calculate and append MSE for BVAR forecasts
  #rmse_results$BVAR_spot <- c(rmse_results$BVAR_spot, sqrt(mean((bvar_rev_fcs_spot - act_spot)^2)))
  #rmse_results$BVAR_forwp <- c(rmse_results$BVAR_forwp, sqrt(mean((bvar_rev_fcs_forwp - act_forwp)^2)))
  
  
  # Calculate and append MSE for VECM forecasts
  rmse_results$VECM_spot <- c(rmse_results$VECM_spot, sqrt(mean((vecm_fcs$fcst[[1]][, "fcst"] - act_spot)^2)))
  rmse_results$VECM_forwp <- c(rmse_results$VECM_forwp, sqrt(mean((vecm_fcs$fcst[[2]][, "fcst"] - act_forwp)^2)))
  
  # Calculate and append MSE for ARIMA forecasts
  rmse_results$ARIMA_spot <- c(rmse_results$ARIMA_spot, sqrt(mean((arima_fcs_spot$mean - act_spot)^2)))
  rmse_results$ARIMA_forwp <- c(rmse_results$ARIMA_forwp, sqrt(mean((arima_fcs_forwp$mean - act_forwp)^2)))
  
  # Calculate and append MSE for Random Walk forecasts
  rmse_results$RW_spot <- c(rmse_results$RW_spot, sqrt(mean((rw_fcs_spot$mean - act_spot)^2)))
  rmse_results$RW_forwp <- c(rmse_results$RW_forwp, sqrt(mean((rw_fcs_forwp$mean - act_forwp)^2)))
  
  
  
  # Calculate and append MAE for VAR forecasts
  mae_results$VAR_spot <- c(mae_results$VAR_spot, mean(abs(var_rev_fcs_spot - act_spot)))
  mae_results$VAR_forwp <- c(mae_results$VAR_forwp, mean(abs(var_rev_fcs_forwp - act_forwp)))
  
  # Calculate and append MAE for VARX forecasts
  #mae_results$VARX_spot <- c(mae_results$VARX_spot, mean(abs(varx_rev_fcs_spot - act_spot)))
  #mae_results$VARX_forwp <- c(mae_results$VARX_forwp, mean(abs(varx_rev_fcs_forwp - act_forwp)))
  
  # Calculate and append MAE for BVAR forecasts
  #mae_results$BVAR_spot <- c(mae_results$BVAR_spot, mean(abs(bvar_rev_fcs_spot - act_spot)))
  #mae_results$BVAR_forwp <- c(mae_results$BVAR_forwp, mean(abs(bvar_rev_fcs_forwp - act_forwp)))
  
  # Calculate and append MAE for VECM forecasts
  mae_results$VECM_spot <- c(mae_results$VECM_spot, mean(abs(vecm_fcs$fcst[[1]][, "fcst"] - act_spot)))
  mae_results$VECM_forwp <- c(mae_results$VECM_forwp, mean(abs(vecm_fcs$fcst[[2]][, "fcst"] - act_forwp)))
  
  # Calculate and append MAE for ARIMA forecasts
  mae_results$ARIMA_spot <- c(mae_results$ARIMA_spot, mean(abs(arima_fcs_spot$mean - act_spot)))
  mae_results$ARIMA_forwp <- c(mae_results$ARIMA_forwp, mean(abs(arima_fcs_forwp$mean - act_forwp)))
  
  # Calculate and append MAE for Random Walk forecasts
  mae_results$RW_spot <- c(mae_results$RW_spot, mean(abs(rw_fcs_spot$mean - act_spot)))
  mae_results$RW_forwp <- c(mae_results$RW_forwp, mean(abs(rw_fcs_forwp$mean - act_forwp)))
  
  
  # Calculate Theil
  
  theil_results$VAR_spot <- c(theil_results$VAR_spot, calculate_theils_u(forecast = var_rev_fcs_spot, actual = act_spot))
  
  # Continuing Theil's U calculations for other models
  # VAR forwp
  theil_results$VAR_forwp <- c(theil_results$VAR_forwp, calculate_theils_u(forecast = var_rev_fcs_forwp, actual = act_forwp))
  # VARX spot
  # theil_results$VARX_spot <- c(theil_results$VARX_spot, calculate_theils_u(forecast = varx_rev_fcs_spot, actual = act_spot))
  # VARX forwp
  #theil_results$VARX_forwp <- c(theil_results$VARX_forwp, calculate_theils_u(forecast = varx_rev_fcs_forwp, actual = act_forwp))
  # BVAR spot
  #theil_results$BVAR_spot <- c(theil_results$BVAR_spot, calculate_theils_u(forecast = bvar_rev_fcs_spot, actual = act_spot))
  # BVAR forwp
  #theil_results$BVAR_forwp <- c(theil_results$BVAR_forwp, calculate_theils_u(forecast = bvar_rev_fcs_forwp, actual = act_forwp))
  # VECM spot
  theil_results$VECM_spot <- c(theil_results$VECM_spot, calculate_theils_u(forecast = vecm_fcs$fcst[[1]][, "fcst"], actual = act_spot))
  # VECM forwp
  theil_results$VECM_forwp <- c(theil_results$VECM_forwp, calculate_theils_u(forecast = vecm_fcs$fcst[[2]][, "fcst"], actual = act_forwp))
  # ARIMA spot
  theil_results$ARIMA_spot <- c(theil_results$ARIMA_spot, calculate_theils_u(forecast = arima_fcs_spot$mean, actual = act_spot))
  # ARIMA forwp
  theil_results$ARIMA_forwp <- c(theil_results$ARIMA_forwp, calculate_theils_u(forecast = arima_fcs_forwp$mean, actual = act_forwp))
  # Random Walk spot
  theil_results$RW_spot <- c(theil_results$RW_spot, calculate_theils_u(forecast = rw_fcs_spot$mean, actual = act_spot))
  # Random Walk forwp
  theil_results$RW_forwp <- c(theil_results$RW_forwp, calculate_theils_u(forecast = rw_fcs_forwp$mean, actual = act_forwp))
  
  
  # Calculate and append MAPE for VAR forecasts
  mape_results$VAR_spot <- c(mape_results$VAR_spot, calculate_mape(act_spot, var_rev_fcs_spot))
  mape_results$VAR_forwp <- c(mape_results$VAR_forwp, calculate_mape(act_forwp, var_rev_fcs_forwp))
  
  # Calculate and append MAPE for VARX forecasts
  #mape_results$VARX_spot <- c(mape_results$VARX_spot, calculate_mape(act_spot, varx_rev_fcs_spot))
  #mape_results$VARX_forwp <- c(mape_results$VARX_forwp, calculate_mape(act_forwp, varx_rev_fcs_forwp))
  
  # Calculate and append MAPE for BVAR forecasts
  #mape_results$BVAR_spot <- c(mape_results$BVAR_spot, calculate_mape(act_spot, bvar_rev_fcs_spot))
  #mape_results$BVAR_forwp <- c(mape_results$BVAR_forwp, calculate_mape(act_forwp, bvar_rev_fcs_forwp))
  
  # Calculate and append MAPE for VECM forecasts
  mape_results$VECM_spot <- c(mape_results$VECM_spot, calculate_mape(act_spot, vecm_fcs$fcst[[1]][, "fcst"]))
  mape_results$VECM_forwp <- c(mape_results$VECM_forwp, calculate_mape(act_forwp, vecm_fcs$fcst[[2]][, "fcst"]))
  
  # Calculate and append MAPE for ARIMA forecasts
  mape_results$ARIMA_spot <- c(mape_results$ARIMA_spot, calculate_mape(act_spot, arima_fcs_spot$mean))
  mape_results$ARIMA_forwp <- c(mape_results$ARIMA_forwp, calculate_mape(act_forwp, arima_fcs_forwp$mean))
  
  # Calculate and append MAPE for Random Walk forecasts
  mape_results$RW_spot <- c(mape_results$RW_spot, calculate_mape(act_spot, rw_fcs_spot$mean))
  mape_results$RW_forwp <- c(mape_results$RW_forwp, calculate_mape(act_forwp, rw_fcs_forwp$mean))
  
  
  
}

# Average Direction Accuracy Calculation
average_direction_accuracy <- list()
for (model_name in names(direction_accuracy_results)) {
  average_direction_accuracy[[model_name]] <- mean(direction_accuracy_results[[model_name]]) * 100
}

# Error Metrics Calculation
mean_rmse_results <- sapply(rmse_results, mean)
mean_mae_results <- sapply(mae_results, mean)
mean_theil_results <- sapply(theil_results, mean)
mean_mape_results <- sapply(mape_results, mean)

# RMSE Reduction from RW Calculation
rmse_reduction_from_rw <- list(
  VAR_spot = numeric(),
  VAR_forwp = numeric(),
  ARIMA_spot = numeric(),
  ARIMA_forwp = numeric(),
  RW_spot = numeric(),
  RW_forwp = numeric(),
  VECM_spot = numeric(),
  VECM_forwp = numeric()
  #BVAR_spot = numeric(),
  #BVAR_forwp = numeric(),
  #VARX_spot = numeric(),
  #VARX_forwp = numeric()
)

for(model_name in names(mean_rmse_results)) {
  if(!model_name %in% c("RW_spot", "RW_forwp")) {
    rw_baseline <- ifelse(grepl("spot", model_name), "RW_spot", "RW_forwp")
    rmse_reduction_from_rw[[model_name]] <- (1 - mean_rmse_results[[model_name]] / mean_rmse_results[[rw_baseline]]) * 100
  }
}

# Aggregating Forecasts and Actuals
aggregated_forecasts <- list()
aggregated_actuals <- list(
  spot = unlist(lapply(actual_values_list, `[[`, "spot")),
  forwp = unlist(lapply(actual_values_list, `[[`, "forwp"))
)

models <- c("VAR", "ARIMA", "VECM", "RW")
for(model in models) {
  aggregated_forecasts[[model]] <- list(
    spot = unlist(lapply(forecasted_values, function(x) x[[model]][["spot"]])),
    forwp = unlist(lapply(forecasted_values, function(x) x[[model]][["forwp"]]))
  )
}

# Diebold-Mariano Test
dm_test_results <- list()
for(model in models) {
  if(model != "RW") {
    for(market in c("spot", "forwp")) {
      forecast1 <- aggregated_forecasts[[model]][[market]]
      forecast2 <- aggregated_forecasts[["RW"]][[market]]
      actual <- aggregated_actuals[[market]]
      errors1 <- forecast1 - actual
      errors2 <- forecast2 - actual
      dm_test_results[[paste(model, market, "vs_RW", market, sep = "_")]] <- dm.test(errors1, errors2)
    }
  }
}

# Printing Results
cat("=== Average Direction Accuracy for Each Model (%) ===\n")
for(model_name in names(average_direction_accuracy)) {
  cat(model_name, ":", sprintf("%.2f%%", average_direction_accuracy[[model_name]]), "\n")
}

cat("\n=== Mean RMSE Results ===\n")
for(model_name in names(mean_rmse_results)) {
  cat(model_name, ":", sprintf("%.4f", mean_rmse_results[[model_name]]), "\n")
}

cat("\n=== Mean MAE Results ===\n")
for(model_name in names(mean_mae_results)) {
  cat(model_name, ":", sprintf("%.4f", mean_mae_results[[model_name]]), "\n")
}

cat("\n=== Mean Theil Results ===\n")
for(model_name in names(mean_theil_results)) {
  cat(model_name, ":", sprintf("%.4f", mean_theil_results[[model_name]]), "\n")
}

cat("\n=== Mean MAPE Results ===\n")
for(model_name in names(mean_mape_results)) {
  cat(model_name, ":", sprintf("%.2f%%", mean_mape_results[[model_name]]*100), "\n")
}

cat("\n=== RMSE Reduction from RW (%) ===\n")
for(model_name in names(rmse_reduction_from_rw)) {
  cat(model_name, ":", sprintf("%.2f%%", rmse_reduction_from_rw[[model_name]]), "\n")
}

cat("\n=== Diebold-Mariano Test Results (Statistic and P-Value) ===\n")
for(result in names(dm_test_results)) {
  cat(result, ": Statistic =", sprintf("%.4f", dm_test_results[[result]]$statistic),
      ", P-Value =", sprintf("%.4f", dm_test_results[[result]]$p.value), "\n")
}
