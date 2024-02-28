#install.packages("readr")
#install.packages("dplyr")
#install.packages("lubridate")
#install.packages("tseries")
#install.packages("forecast")
#install.packages("vars")
#install.packages("urca")
#install.packages("tsDyn")

#getwd()
#setwd("./master-thesis")
library(readr)  # For reading CSV files
library(dplyr)  # For data manipulation
library(lubridate)  # For date parsing
library(tseries)
library(vars)
library(forecast)
library(urca)
library(tsDyn)



# STEP 1: READ CSV
spot_prices <- read_delim('./data/spot/panamax/BAPI_historical.csv', 
                          delim = ';', 
                          escape_double = FALSE, 
                          col_types = cols(Date = col_date(format = "%d.%m.%Y")),
                          trim_ws = TRUE)

forward_prices <- read_delim('./data/ffa/panamax/4tc_data.csv', 
                             delim = ';', 
                             escape_double = FALSE, 
                             col_types = cols(Date = col_date(format = "%d.%m.%Y")),
                             trim_ws = TRUE)






# STEP 2: CLEAN AND PREPARE DATA


# Merge data frames on the Date column
data_combined <- merge(spot_prices, forward_prices, by = "Date")


# Transform data to log levels and create a new data frame for log levels
data_log_levels <- data.frame(
  Date = data_combined$Date,
  spot = log(data_combined$Open),
  forwc = log(data_combined$`4TC_PCURMON`),
  forw1m = log(data_combined$`4TC_P+1MON`),
  forwp = log(data_combined$`4TC_ROLL`),
  wc = data_combined$`w C`,
  w1m = data_combined$`w 1M`
)


# Display the first few rows of each new data frame to verify
print(head(data_log_levels))

# Split into train and test sets

# Calculate the index for the split
split_index <- round(nrow(data_combined) * 0.8)

# Split the data into training and test sets
train_lev <- data_log_levels[1:split_index, ]
test_lev <- data_log_levels[(split_index+1): nrow(data_log_levels), ]



# Calculate differences

# Calculate first differences for the training set
train_diff <- data.frame(
  Date = train_lev$Date[-1],  # Exclude the first date because there's no prior value to difference with
  spot = diff(train_lev$spot),
  forwc = diff(train_lev$forwc),
  forw1m = diff(train_lev$forw1m),
  forwp = diff(train_lev$forwp),
  wc = train_lev$wc[-1],
  w1m = train_lev$w1m[-1]
)

# Print the first few rows to verify
print(head(train_diff))




# Prepare time series objects

train_lev_ts <- ts(train_lev[, c(2, 5)])
train_diff_ts <- ts(train_diff[, c(2, 5)])

#test_log_levels_ts <- ts(test_log_levels[, -1])








# Step 3: STATIONARITY CHECKS

# Perform Augmented Dickey-Fuller Test
lapply(train_lev_ts, function(series) adf.test(series, alternative = "stationary"))
lapply(train_diff_ts, function(series) adf.test(series, alternative = "stationary"))






# Step 4: MODEL FITS AND DIAGNOSTIC CHECKS

# VAR
# Select an appropriate lag order, p. You can use the VARselect function for guidance
lag_order <- VARselect(train_diff_ts, type = "both")$selection["AIC(n)"]
var_model <- VAR(train_diff_ts, p = lag_order, type = "both")
summary(var_model)

# Extract the residuals from the VAR model
residuals_var <- residuals(var_model)

# Get the number of equations (variables) in the VAR model
num_equations <- ncol(residuals_var)

arch_var <- arch.test(var_model)
arch_var

stab_var <- stability(var_model, type = "OLS-CUSUM")
plot(stab_var)

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
#var_irf_spot_to_spot <- irf(var_model, impulse = "spot", response = "spot")
#plot(var_irf_spot_to_spot, main = "Shock from 'spot' to 'spot'")

# 2. Shock from "spot" to "forwc"
#var_irf_spot_to_forwc <- irf(var_model, impulse = "spot", response = "forwc")
#plot(var_irf_spot_to_forwc, main = "Shock from 'spot' to 'forwc'")

# 3. Shock from "spot" to "forw1m"
#var_irf_spot_to_forw1m <- irf(var_model, impulse = "spot", response = "forw1m")
#plot(var_irf_spot_to_forw1m, main = "Shock from 'spot' to 'forw1m'")

# 4. Shock from "forwc" to "spot"
#var_irf_forwc_to_spot <- irf(var_model, impulse = "forwc", response = "spot")
#plot(var_irf_forwc_to_spot, main = "Shock from 'forwc' to 'spot'")

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

var_vd1 <- fevd(var_model)
plot(var_vd1)



# VECM
#coint_test <- ca.jo(train_log_diff_ts, spec = "transitory", type = "eigen", ecdet = "const", K = lag_order)
#summary(coint_test)

#vecm_model <- VECM(train_log_diff_ts, lag=lag_order, r=1, include="none")
#summary(vecm_model)
# Impulse response analysis
#irf.vecm <- irf(vecm_model, n.ahead=10, boot=TRUE)

# Plot the impulse responses
#plot(irf.vecm)

# ARIMA
arima_model_spot <- auto.arima(train_lev$spot)
arima_model_forwc <- auto.arima(train_lev$forwc)
arima_model_forw1m <- auto.arima(train_lev$forw1m)

arima_res_spot <- residuals(arima_model_spot)
arima_res_forw <- residuals(arima_model_forwc)
arima_res_forw2 <- residuals(arima_model_forw1m)

Box.test(arima_res_spot, type = "Ljung-Box")
Box.test(arima_res_forw, type = "Ljung-Box")
Box.test(arima_res_forw2, type = "Ljung-Box")


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


# Fit a linear model to the residuals of the ARIMA model for log_4TC_FORWARD
lm_res_forw2 <- lm(arima_res_forw2 ~ I(1:nrow(as.data.frame(arima_res_forw2))))

# Conduct the White test for heteroskedasticity
white_test_forw2 <- bptest(lm_res_forw2)

# Print the results for log_4TC_FORWARD
print("White test for heteroskedasticity in ARIMA model residuals (log_4TC_FORWARD2):")
print(white_test_forw2)



# Step 9: Forecast future values
forecast_horizon <- 10
#vecm_forecasts <- predict(vecm_model, n.ahead = forecast_horizon)
var_fcs <- predict(var_model, n.ahead = forecast_horizon) 

#n_forecast <- nrow(test_log_levels)  # Number of points to forecast equals the size of the test set

# Determine the number of rounds based on the test set size and forecast horizon
num_rounds <- min(floor(nrow(test_lev) / forecast_horizon), 30)
num_rounds <- floor(nrow(test_lev) / forecast_horizon)

print(num_rounds)

# Initialize lists to store MSE results for each model
mse_results <- list(
  VAR_spot = numeric(),
  VAR_forwp = numeric(),
  VAR_forwc = numeric(),
  VAR_forw1m = numeric(),
  ARIMA_spot = numeric(),
  ARIMA_forwp = numeric(),
  ARIMA_forwc = numeric(),
  ARIMA_forw1m = numeric(),
  RW_spot = numeric(),
  RW_forwp = numeric(),
  RW_forwc = numeric(),
  RW_forw1m = numeric()
  #VECM_Spot = numeric(),
  #VECM_4TC_FORWARD = numeric()
)


arima_fcs_spot <- forecast(arima_model_spot, h = forecast_horizon)
arima_fcs_forwc <- forecast(arima_model_forwc, h = forecast_horizon)
arima_fcs_forw1m <- forecast(arima_model_forw1m, h = forecast_horizon)

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
    forwc = diff(train_lev$forwc),
    forw1m = diff(train_lev$forw1m),
    forwp = diff(train_lev$forwp),
    wc = train_lev$wc[-1],
    w1m = train_lev$w1m[-1]
  )
  
  # Re-fit models with the updated training set
  train_diff_ts <- ts(train_diff[, c(2, 5)])
  
  
  ## VAR
  lag_order <- VARselect(train_diff_ts, type = "both")$selection["AIC(n)"]
  var_model <- VAR(train_diff_ts, p = lag_order, type = "both")
  var_fcs <- predict(var_model, n.ahead = forecast_horizon)  
  
  # VECM
  #vecm_model <- VECM(current_train_log_diff_ts, lag=lag_order, r=1, include="none")
  #vecm_forecasts <- predict(vecm_model, n.ahead = forecast_horizon)
  
  
  
  ## ARIMA
  arima_model_spot <- auto.arima(train_lev$spot)
  arima_model_forwp <- auto.arima(train_lev$forwp)
  arima_model_forwc <- auto.arima(train_lev$forwc)
  arima_model_forw1m <- auto.arima(train_lev$forw1m)
  
  arima_fcs_spot <- forecast(arima_model_spot, h = forecast_horizon)
  arima_fcs_forwp <- forecast(arima_model_forwp, h = forecast_horizon)
  arima_fcs_forwc <- forecast(arima_model_forwc, h = forecast_horizon)
  arima_fcs_forw1m <- forecast(arima_model_forw1m, h = forecast_horizon)
  
  
  # Random Walk (ARIMA(0,1,0))
  rw_model_spot <- Arima(train_lev$spot, order = c(0, 1, 0))
  rw_model_forwp <- Arima(train_lev$forwp, order = c(0, 1, 0))
  rw_model_forwc <- Arima(train_lev$forwc, order = c(0, 1, 0))
  rw_model_forw1m <- Arima(train_lev$forw1m, order = c(0, 1, 0))
  
  rw_fcs_spot <- forecast(rw_model_spot, h = forecast_horizon)
  rw_fcs_forwp <- forecast(rw_model_forwp, h = forecast_horizon)
  rw_fcs_forwc <- forecast(rw_model_forwc, h = forecast_horizon)
  rw_fcs_forw1m <- forecast(rw_model_forw1m, h = forecast_horizon)
  
  
  # Step 10: Convert differenced forecasts back to levels
  
  # Last log levels from the training set
  last_spot <- tail(train_lev$spot, 1)
  last_forwp <- tail(train_lev$forwp, 1)
  last_forwc <- tail(train_lev$forwc, 1)
  last_forw1m <- tail(train_lev$forw1m, 1)
  
  # VAR
  var_rev_fcs_spot <- last_spot + cumsum(var_fcs$fcst[[1]][, "fcst"])
  var_rev_fcs_forwp <- last_forwp + cumsum(var_fcs$fcst[[2]][, "fcst"])
  #var_rev_fcs_forw1m <- last_forw1m + cumsum(var_fcs$fcst[[3]][, "fcst"])
  
  # VECM
  #vecm_reverted_forecasts_log_Spot <- last_log_spot + cumsum(vecm_forecasts[, 1])
  #vecm_reverted_forecasts_log_4TC_FORWARD <- last_log_4TC_FORWARD + cumsum(vecm_forecasts[, 2])
  
  
  
  act_spot <- test_lev$spot
  act_forwp <- test_lev$forwp
  act_forwc <- test_lev$forwc
  act_forw1m <- test_lev$forw1m
  
  # Calculate and append MSE for VAR forecasts
  mse_results$VAR_spot <- c(mse_results$VAR_spot, mean((var_rev_fcs_spot - act_spot)^2))
  mse_results$VAR_forwc <- c(mse_results$VAR_forwp, mean((var_rev_fcs_forwp - act_forwp)^2))
  #mse_results$VAR_forwc <- c(mse_results$VAR_forwc, mean((var_rev_fcs_forwc - act_forwc)^2))
  #mse_results$VAR_forw1m <- c(mse_results$VAR_forw1m, mean((var_rev_fcs_forw1m - act_forw1m)^2))
  
  # Calculate and append MSE for ARIMA forecasts
  mse_results$ARIMA_spot <- c(mse_results$ARIMA_spot, mean((arima_fcs_spot$mean - act_spot)^2))
  mse_results$ARIMA_forwp <- c(mse_results$ARIMA_forwp, mean((arima_fcs_forwp$mean - act_forwp)^2))
  mse_results$ARIMA_forwc <- c(mse_results$ARIMA_forwc, mean((arima_fcs_forwc$mean - act_forwc)^2))
  mse_results$ARIMA_forw1m <- c(mse_results$ARIMA_forw1m, mean((arima_fcs_forw1m$mean - act_forw1m)^2))
  
  # Calculate and append MSE for Random Walk forecasts
  mse_results$RW_spot <- c(mse_results$RW_spot, mean((rw_fcs_spot$mean - act_spot)^2))
  mse_results$RW_forwp <- c(mse_results$RW_forwp, mean((rw_fcs_forwp$mean - act_forwp)^2))
  mse_results$RW_forwc <- c(mse_results$RW_forwc, mean((rw_fcs_forwc$mean - act_forwc)^2))
  mse_results$RW_forw1m <- c(mse_results$RW_forw1m, mean((rw_fcs_forw1m$mean - act_forw1m)^2))
}

# Calculate mean MSE for each model
mean_mse_results <- sapply(mse_results, mean)
# Now multiply all mean MSE results by 100
mean_mse_results <- mean_mse_results * 100

# Print mean MSE results
cat("Mean MSE Results:\n")
print(mean_mse_results)