install.packages("readr")
install.packages("dplyr")
install.packages("lubridate")
install.packages("tseries")
install.packages("forecast")


library(readr)  # For reading CSV files
library(dplyr)  # For data manipulation
library(lubridate)  # For date parsing

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

# The mutate step to convert dates is integrated into read_delim through col_types

# Step 2: Merge data frames on the Date column
data_combined <- merge(spot_prices, forward_prices, by = "Date")

# Check the combined data
print(head(data_combined))


# Step 3: Convert the data frame to a time series (zoo) object
data_ts <- zoo(data_combined[, c("Open", "4TC_FORWARD")], order.by = data_combined$Date)

# Step 4: Check for stationarity and make any necessary adjustments
# This step is crucial for VAR modeling but is skipped here for brevity.
# You might use functions like `adf.test` from the tseries package to check for stationarity
# and differences or transformations if needed.

library(tseries)

# Assuming data_combined is your merged and prepared DataFrame
# Convert to time series
open_ts <- ts(data_combined$Open)
forward_ts <- ts(data_combined$`4TC_FORWARD`)

# Perform Augmented Dickey-Fuller Test
adf_test_open <- adf.test(open_ts, alternative = "stationary")
adf_test_forward <- adf.test(forward_ts, alternative = "stationary")

# Print the results
print(adf_test_open)
print(adf_test_forward)

# Convert to log differences to stabilize variance and potentially achieve stationarity
log_diff_data <- diff(log(data_ts))

# Since diff() reduces the length of the series by 1, adjust the dates accordingly
dates_adj <- index(data_ts)[-1]  # Exclude the first date because diff() operation reduces the length by 1

# Convert the log-differenced data back to a ts object with adjusted dates
log_diff_ts <- zoo(log_diff_data, order.by = dates_adj)

# Perform Augmented Dickey-Fuller Test on the log-differenced series
adf_test_open <- adf.test(log_diff_ts[,1], alternative = "stationary")
adf_test_forward <- adf.test(log_diff_ts[,2], alternative = "stationary")

# Print the ADF test results
cat("ADF Test for Open (Log-Differenced):\n")
print(adf_test_open)
cat("\nADF Test for 4TC_FORWARD (Log-Differenced):\n")
print(adf_test_forward)


# Step 5: Fit the VAR model
# Select an appropriate lag order, p. You can use the `VARselect` function for guidance
lag_order <- VARselect(log_diff_ts, type = "both")$selection["AIC(n)"]
var_model <- VAR(log_diff_ts, p = lag_order, type = "both")

# Step 6: Forecast future values
forecasts <- predict(var_model, n.ahead = 10)

# Print the forecast
print(forecasts$fcst)


# BENCHMARKS

library(forecast)

# Fit ARIMA model on the log-differenced "Open" series
arima_model <- auto.arima(log_diff_ts[,1])

# Forecast future values
arima_forecasts <- forecast(arima_model, h=10)
print(arima_forecasts$fcst)


# Convert forecasts back to the original scale if you applied log-difference
# This step is omitted here for simplicity but might be necessary depending on your specific use case

# Assuming the last observed value of the "Open" series
last_observed_value <- tail(log_diff_ts[,1], 1)

# Simulate Random Walk forecasts
# Here, simply replicate the last observed value for the forecast horizon
random_walk_forecasts <- rep(last_observed_value, 10)


# COMPARISON

# Compute MSE for VAR Model
mse_var <- mean((forecasts$fcst[[1]][,1] - actuals)^2)

# Compute MSE for ARIMA Model
# Make sure `actuals` is on the appropriate scale if you reverted the log-difference transformation
mse_arima <- mean((arima_forecasts$mean - actuals)^2)

# Compute MSE for Random Walk
mse_random_walk <- mean((random_walk_forecasts - actuals)^2)

# Print MSEs
cat("MSE - VAR Model:", mse_var, "\n")
cat("MSE - ARIMA Model:", mse_arima, "\n")
cat("MSE - Random Walk:", mse_random_walk, "\n")




