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

forward_prices <- read_delim('./data/ffa/panamax/4tc_forward.csv', 
                             delim = ';', 
                             escape_double = FALSE, 
                             col_types = cols(Date = col_date(format = "%Y-%m-%d")),
                             trim_ws = TRUE)


perp_forward_prices <- read_delim('./data/ffa/panamax/4tc_perpetual.csv', 
                             delim = ';', 
                             escape_double = FALSE, 
                             col_types = cols(Date = col_date(format = "%d/%m/%Y")),
                             trim_ws = TRUE)







# STEP 2: CLEAN AND PREPARE DATA


# Merge data frames on the Date column
data_combined <- merge(spot_prices, perp_forward_prices, by = "Date")


# Transform data to log levels and create a new data frame for log levels
data_log_levels <- data.frame(
  Date = data_combined$Date,
  log_Spot = log(data_combined$Open),
  log_4TC_FORWARD = log(data_combined$`Perpetual`)
)

# Display the first few rows of each new data frame to verify
print(head(data_log_levels))

# Split into train and test sets

# Calculate the index for the split
split_index <- round(nrow(data_combined) * 0.5)

# Split the data into training and test sets
train_log_levels <- data_log_levels[1:split_index, ]
test_log_levels <- data_log_levels[(split_index+1): nrow(data_log_levels), ]



# Calculate differences

# Calculate first differences for the training set
train_log_diff <- data.frame(
  Date = train_log_levels$Date[-1],  # Exclude the first date because there's no prior value to difference with
  diff_log_Spot = diff(train_log_levels$log_Spot),
  diff_log_4TC_FORWARD = diff(train_log_levels$log_4TC_FORWARD)
)

# Print the first few rows to verify
print(head(train_log_diff))




# Prepare time series objects

train_log_levels_ts <- ts(train_log_levels[, -1])
train_log_diff_ts <- ts(train_log_diff[, -1])

#test_log_levels_ts <- ts(test_log_levels[, -1])








# Step 3: STATIONARITY CHECKS

# Perform Augmented Dickey-Fuller Test
lapply(train_log_levels_ts, function(series) adf.test(series, alternative = "stationary"))
lapply(train_log_diff_ts, function(series) adf.test(series, alternative = "stationary"))






# Step 4: MODEL FITS AND DIAGNOSTIC CHECKS

# VAR
# Select an appropriate lag order, p. You can use the `VARselect` function for guidance
lag_order <- VARselect(train_log_diff_ts, type = "both")$selection["AIC(n)"]
var_model <- VAR(train_log_diff_ts, p = lag_order, type = "both")
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
spot_ts <- train_log_diff_ts[, 1]
forw_ts <- train_log_diff_ts[, 2]
grangerSpot <- causality(var_model, cause = "diff_log_Spot")
grangerSpot
grangerForw <- causality(var_model, cause = "diff_log_4TC_FORWARD")
grangerForw

# Impulse response
var_irf1 <- irf(var_model, impulse = "diff_log_Spot", response = "diff_log_4TC_FORWARD")
plot(var_irf1, ylab="Spot", main = "Shock from FORWARD")
var_irf2 <- irf(var_model, impulse = "diff_log_4TC_FORWARD", response = "diff_log_Spot")
plot(var_irf2, ylab = "Forward", main = "Shock from Spot")


# Variance decomposition

var_vd1 <- fevd(var_model)
plot(var_vd1)



# VECM
coint_test <- ca.jo(train_log_diff_ts, spec = "transitory", type = "eigen", ecdet = "const", K = lag_order)
summary(coint_test)

vecm_model <- VECM(train_log_diff_ts, lag=lag_order, r=1, include="none")
summary(vecm_model)
# Impulse response analysis
irf.vecm <- irf(vecm_model, n.ahead=10, boot=TRUE)

# Plot the impulse responses
plot(irf.vecm)

# ARIMA
arima_model_spot <- auto.arima(train_log_levels$log_Spot)
arima_model_4TC_FORWARD <- auto.arima(train_log_levels$log_4TC_FORWARD)

arima_res_spot <- residuals(arima_model_spot)
arima_res_forw <- residuals(arima_model_4TC_FORWARD)

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
forecast_horizon <- 30
vecm_forecasts <- predict(vecm_model, n.ahead = forecast_horizon)
var_forecasts <- predict(var_model, n.ahead = forecast_horizon) 

#n_forecast <- nrow(test_log_levels)  # Number of points to forecast equals the size of the test set

# Determine the number of rounds based on the test set size and forecast horizon
num_rounds <- min(floor(nrow(test_log_levels) / forecast_horizon), 30)
print(num_rounds)

# Initialize lists to store MSE results for each model
mse_results <- list(
  VAR_Spot = numeric(),
  VAR_4TC_FORWARD = numeric(),
  ARIMA_Spot = numeric(),
  ARIMA_4TC_FORWARD = numeric(),
  RW_Spot = numeric(),
  RW_4TC_FORWARD = numeric(),
  VECM_Spot = numeric(),
  VECM_4TC_FORWARD = numeric()
)


arima_forecasts_spot <- forecast(arima_model_spot, h = forecast_horizon)
arima_forecasts_4TC_FORWARD <- forecast(arima_model_4TC_FORWARD, h = forecast_horizon)


# Loop through each forecasting round
for (round in 1:num_rounds) {
  cat("Round", round)
  # Update the end index of the training set for this round
  if (round == 1){
    split_index <- split_index
  }
  else{
    split_index <- split_index + forecast_horizon
    
  }
  current_train_log_levels <- data_log_levels[1:split_index, ]
  current_test_log_levels <- data_log_levels[(split_index + 1):(split_index + forecast_horizon), ]

  
  # Recalculate differences for the updated training set
  current_train_log_diff <- data.frame(
    Date = current_train_log_levels$Date[-1],
    diff_log_Spot = diff(current_train_log_levels$log_Spot),
    diff_log_4TC_FORWARD = diff(current_train_log_levels$log_4TC_FORWARD)
  )
  
  # Re-fit models with the updated training set
  current_train_log_diff_ts <- ts(current_train_log_diff[, -1])
  
  ## VAR
  lag_order <- VARselect(current_train_log_diff_ts, type = "both")$selection["AIC(n)"]
  var_model <- VAR(current_train_log_diff_ts, p = lag_order, type = "both")
  var_forecasts <- predict(var_model, n.ahead = forecast_horizon)  # Forecasting only 1 step ahead but with updated training set each time
  
  # VECM
  vecm_model <- VECM(current_train_log_diff_ts, lag=lag_order, r=1, include="none")
  vecm_forecasts <- predict(vecm_model, n.ahead = forecast_horizon)
  

  
  ## ARIMA
  arima_model_spot <- auto.arima(current_train_log_levels$log_Spot)
  arima_forecasts_spot <- forecast(arima_model_spot, h = forecast_horizon)  # Only 1 step ahead

  arima_model_4TC_FORWARD <- auto.arima(current_train_log_levels$log_4TC_FORWARD)
  arima_forecasts_4TC_FORWARD <- forecast(arima_model_4TC_FORWARD, h = forecast_horizon)  # Only 1 step ahead
  
  # Random Walk (ARIMA(0,1,0))
  rw_model_spot <- Arima(current_train_log_levels$log_Spot, order = c(0, 1, 0))
  rw_forecasts_spot <- forecast(rw_model_spot, h = forecast_horizon)

  rw_model_4TC_FORWARD <- Arima(current_train_log_levels$log_4TC_FORWARD, order = c(0, 1, 0))
  rw_forecasts_4TC_FORWARD <- forecast(rw_model_4TC_FORWARD, h = forecast_horizon)
  
  
  # Step 10: Convert differenced forecasts back to levels
  
  # Last log levels from the training set
  last_log_spot <- tail(current_train_log_levels$log_Spot, 1)
  last_log_4TC_FORWARD <- tail(current_train_log_levels$log_4TC_FORWARD, 1)
  
  # VAR
  var_reverted_forecasts_log_Spot <- last_log_spot + cumsum(var_forecasts$fcst[[1]][, "fcst"])
  var_reverted_forecasts_log_4TC_FORWARD <- last_log_4TC_FORWARD + cumsum(var_forecasts$fcst[[2]][, "fcst"])
  
  # VECM
  vecm_reverted_forecasts_log_Spot <- last_log_spot + cumsum(vecm_forecasts[, 1])
  vecm_reverted_forecasts_log_4TC_FORWARD <- last_log_4TC_FORWARD + cumsum(vecm_forecasts[, 2])
  
  
  # Append MSE results for each model
  actual_log_spot <- current_test_log_levels$log_Spot
  actual_log_4TC_FORWARD <- current_test_log_levels$log_4TC_FORWARD
  
  
  mse_results$VAR_Spot <- c(mse_results$VAR_Spot, mean((var_reverted_forecasts_log_Spot - actual_log_spot)^2))
  mse_results$VAR_4TC_FORWARD <- c(mse_results$VAR_4TC_FORWARD, mean((var_reverted_forecasts_log_4TC_FORWARD - actual_log_4TC_FORWARD)^2))
  
  mse_results$VECM_Spot <- c(mse_results$VECM_Spot, mean((vecm_reverted_forecasts_log_Spot - actual_log_spot)^2))
  mse_results$VECM_4TC_FORWARD <- c(mse_results$VECM_4TC_FORWARD, mean((vecm_reverted_forecasts_log_4TC_FORWARD - actual_log_4TC_FORWARD)^2))
  
  mse_results$ARIMA_Spot <- c(mse_results$ARIMA_Spot, mean((arima_forecasts_spot$mean - actual_log_spot)^2))
  mse_results$ARIMA_4TC_FORWARD <- c(mse_results$ARIMA_4TC_FORWARD, mean((arima_forecasts_4TC_FORWARD$mean - actual_log_4TC_FORWARD)^2))
  
  mse_results$RW_Spot <- c(mse_results$RW_Spot, mean((rw_forecasts_spot$mean - actual_log_spot)^2))
  mse_results$RW_4TC_FORWARD <- c(mse_results$RW_4TC_FORWARD, mean((rw_forecasts_4TC_FORWARD$mean - actual_log_4TC_FORWARD)^2))
  
}

# Calculate mean MSE for each model
mean_mse_results <- sapply(mse_results, mean)
# Now multiply all mean MSE results by 100
mean_mse_results <- mean_mse_results * 100

# Print mean MSE results
cat("Mean MSE Results:\n")
print(mean_mse_results)




