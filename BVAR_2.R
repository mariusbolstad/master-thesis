# Install and load necessary packages
library(BVAR)

# Load file path
filepath <- "/Users/andreashaugstvedt/Documents/Project and Master Thesis/Master Thesis/Data/PMX FFA 2005-2023_2.csv"

# Load data
data <- read.csv(filepath, header = TRUE, sep = ";", na.strings = "")  # Adjust the delimiter and other arguments as needed

# Convert data to time series object
ts_data <- ts(data[, -1], start = 1, frequency = 1)  # Assuming the first column is the time index

# Check for missing values
if (anyNA(ts_data)) {
  # Handle missing values (example: mean imputation)
  ts_data[is.na(ts_data)] <- colMeans(ts_data, na.rm = TRUE)
}

# Check if the data is numeric
if (!is.numeric(ts_data)) {
  stop("Data must be numeric")
}

# Specify the lag order for the VAR model
lags <- 2  # Adjust this according to your preference or by using model selection criteria

# Fit Bayesian VAR model
bvar_model <- bvar(ts_data, lags = lags)

# Forecast future values
forecast_horizon <- 12  # Adjust as needed

# Check if there's enough historical data to make forecasts
if (length(ts_data) >= forecast_horizon) {
  forecast <- predict(bvar_model, horizon = forecast_horizon)  # Corrected argument name to 'h'
  
  # Extract predicted values
  predicted_values <- forecast$fcst
  
  # Extract actual values for evaluation
  start_index <- length(ts_data) - forecast_horizon + 1
  if (start_index > 0) {
    actual_values <- ts_data[start_index:length(ts_data), ]
    
    # Calculate Mean Square Error (MSE) for each variable
    mse <- colMeans((predicted_values - actual_values)^2)
    
    # Print MSE for each variable
    print("Mean Square Error (MSE) for each variable:")
    print(mse)
  } else {
    print("Not enough historical data to make forecasts.")
  }
} else {
  print("Not enough historical data to make forecasts.")
}
