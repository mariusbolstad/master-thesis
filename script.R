install.packages("readr")
install.packages("dplyr")
install.packages("lubridate")
install.packages("tseries")
install.packages("forecast")
install.packages("vars")
install.packages("urca")
install.packages("tsDyn")



library(readr)  # For reading CSV files
library(dplyr)  # For data manipulation
library(lubridate)  # For date parsing
library(tseries)
library(vars)
library(forecast)
library(urca)
library(tsDyn)



# Step 1: Correctly read the CSV files with semicolon as separator
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


# Step 2: Merge data frames on the Date column
data_combined <- merge(spot_prices, perp_forward_prices, by = "Date")


# Step 3: Transform data to log levels and create a new data frame for log levels
data_log_levels <- data.frame(
  Date = data_combined$Date,
  log_Spot = log(data_combined$Open),
  log_4TC_FORWARD = log(data_combined$`Perpetual`)
)

# Display the first few rows of each new data frame to verify
print(head(data_log_levels))

# Step 4: Split into train and test sets

# Calculate the index for the split
split_index <- round(nrow(data_combined) * 0.5)

# Split the data into training and test sets
train_log_levels <- data_log_levels[1:split_index, ]
test_log_levels <- data_log_levels[(split_index+1): nrow(data_log_levels), ]



# Step 5: Calculate differences

# Calculate first differences for the training set
train_log_diff <- data.frame(
  Date = train_log_levels$Date[-1],  # Exclude the first date because there's no prior value to difference with
  diff_log_Spot = diff(train_log_levels$log_Spot),
  diff_log_4TC_FORWARD = diff(train_log_levels$log_4TC_FORWARD)
)

# Print the first few rows to verify
print(head(train_log_diff))




# Step 6: Prepare time series objects

train_log_levels_ts <- ts(train_log_levels[, -1])
train_log_diff_ts <- ts(train_log_diff[, -1])

#test_log_levels_ts <- ts(test_log_levels[, -1])


# Step 7: Check for stationarity and make any necessary adjustments

# Perform Augmented Dickey-Fuller Test
lapply(train_log_levels_ts, function(series) adf.test(series, alternative = "stationary"))
lapply(train_log_diff_ts, function(series) adf.test(series, alternative = "stationary"))




# Step 8: Fit the models

# VAR
# Select an appropriate lag order, p. You can use the `VARselect` function for guidance
lag_order <- VARselect(train_log_diff_ts, type = "both")$selection["AIC(n)"]
var_model <- VAR(train_log_diff_ts, p = lag_order, type = "both")
summary(var_model)

# VECM
coint_test <- ca.jo(train_log_diff_ts, spec = "transitory", type = "eigen", ecdet = "const", K = lag_order)
summary(coint_test)

vecm_model <- VECM(train_log_diff_ts, lag=lag_order, r=1, include="none")
summary(vecm_model)
# Impulse response analysis
irf.vecm <- irf(vecm_model, n.ahead=10, boot=TRUE)

# Plot the impulse responses
plot(irf.vecm)






# Convert the Johansen test results into a VECM
#vecm_model <- cajorls(coint_test, r = 1)  # `r` is the number of cointegrating relations
# Summary of the VECM model
#summary(vecm_model$rlm)
# For forecasting, convert VECM to VAR representation
#vecm_to_var <- vec2var(coint_test, r = 1)
#summary(vecm_to_var)


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




