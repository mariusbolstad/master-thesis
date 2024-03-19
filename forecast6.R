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

getwd()
setwd("./master-thesis")
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
gbti_dev <- read_delim('./data/other/GBTI_23022024.csv',
                       delim = ';',
                       escape_double = FALSE,
                       col_types = cols(Date = col_date(format = "%b-%Y")),
                       trim_ws = TRUE,
                       skip = 4)

oecd_ip_dev <- read_delim('./data/other/OECD_IP_23022024.csv',
                       delim = ';',
                       escape_double = FALSE,
                       col_types = cols(Date = col_date(format = "%d/%m/%Y")),
                       trim_ws = TRUE)

fleet_age <- read_delim('./data/other/average_fleet_age_23022024.csv',
                       delim = ';',
                       escape_double = FALSE,
                       col_types = cols(Date = col_date(format = "%b-%Y")),
                       trim_ws = TRUE,
                       skip = 4)

fleet_dev <- read_delim('./data/other/fleet_development_23022024.csv',
                       delim = ';',
                       escape_double = FALSE,
                       col_types = cols(Date = col_date(format = "%b-%Y")),
                       trim_ws = TRUE,
                       skip = 4)

orderbook <- read_delim('./data/other/orderbook_23022024.csv',
                       delim = ';',
                       escape_double = FALSE,
                       col_types = cols(Date = col_date(format = "%b-%Y")),
                       trim_ws = TRUE,
                       skip = 4)

steel_prod <- read_delim('./data/other/steel_production_23022024.csv',
                       delim = ';',
                       escape_double = FALSE,
                       col_types = cols(Date = col_date(format = "%b-%Y")),
                       trim_ws = TRUE,
                       skip = 4)

vessel_sale_volume <- read_delim('./data/other/vessel_sale_volume_23022024.csv',
                       delim = ';',
                       escape_double = FALSE,
                       col_types = cols(Date = col_date(format = "%b-%Y")),
                       trim_ws = TRUE,
                       skip = 4)


# STEP 2: CLEAN AND PREPARE DATA
# CSZ column: SMX
# PMX column: PMX
# SMX column: SMX
# ffa column: ROLL


# Merge data frames on the Date column
data_combined <- merge(spot, csz_forw, by = "Date")


# Remove rows with NA or 0 in either the S6TC_S10TC column or the 4TC_ROLL column
data_combined <- subset(data_combined, !(is.na(CSZ) | CSZ == 0) & !(is.na(`CURMON`) | `CURMON` == 0))


# Transform data to log levels and create a new data frame for log levels
data_log_levels <- data.frame(
  Date = data_combined$Date,
  spot = log(data_combined$CSZ),
  forwp = log(data_combined$`CURMON`)
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
  forwp = diff(train_lev$forwp)
)

# Print the first few rows to verify
print(head(train_diff))




# Prepare time series objects

train_lev_ts <- ts(train_lev[, -1])
train_diff_ts <- ts(train_diff[, -1])

#test_log_levels_ts <- ts(test_log_levels[, -1])


# Step 2a: Clean, prepare and refilter macro data

# It seems like the OECD_IP data must be interpolated on its own as this doesnt follow the regular time intervals as the rest
# As well as GBTI as this doesnt have values before 2015



# Remove all unnecessary rows at bottom (now excluding the OECD and GBTI)
macro_data <- list(fleet_age,fleet_dev,orderbook,steel_prod,vessel_sale_volume)
macro_data_cleaned <- map(macro_data, ~ .x %>% filter(!is.na(Date)))
start_date <- as.Date(train_lev$Date[1])

# Removing all values older than the start date for FFAs
for(i in seq_along(macro_data_cleaned)) {
  macro_var <- macro_data_cleaned[[i]]
  filtered_macro_var <- macro_var %>% filter(Date >= start_date)
  macro_data_cleaned[[i]] <- filtered_macro_var
}

# What follows is the disaggregation of the different macro data sets

# FLEET AGE

fleet_age_clean <- macro_data_cleaned[[1]]
fleet_age_clean$Date <- as.Date(fleet_age_clean$Date)
fleet_age_clean <- xts(fleet_age_clean[,-1], order.by = fleet_age_clean$Date)

fleet_age_daily_list <- list()  # Create an empty list to store the daily time series

for (i in seq(1, ncol(fleet_age_clean))) {
  macro_var <- fleet_age_clean[, i]
  header <- colnames(fleet_age_clean)[i]  # Get the header corresponding to the column
  macro_var_daily <- td(macro_var ~ 1, conversion = "mean", method = "chow-lin-maxlog", to = "day")
  macro_var_daily <- setNames(macro_var_daily$values, header)  # Set the name of the time series to the header
  fleet_age_daily_list[[header]] <- macro_var_daily  # Add the time series to the list with the header as the name
}

fleet_age_daily_df <- data.frame(cbind(fleet_age_daily_list$`Capesize Bulkcarrier Fleet - Average Age [Years]`,
                            fleet_age_daily_list$`Panamax Bulkcarrier Fleet - Average Age [Years]`,
                            fleet_age_daily_list$`Handymax Bulkcarrier Fleet - Average Age [Years]`,
                            fleet_age_daily_list$`Handysize Bulkcarrier Fleet - Average Age [Years]`))

rownames(fleet_age_daily_df) <- format(as.Date(rownames(fleet_age_daily_df)), "%d-%m-%Y")

write.csv(fleet_age_daily_df, "./data/other/fleet_age_daily.csv", row.names = TRUE)


  # FLEET DEVELOPMENT

fleet_dev_clean <- macro_data_cleaned[[2]]
fleet_dev_clean$Date <- as.Date(fleet_dev_clean$Date)
fleet_dev_clean <- xts(fleet_dev_clean[,-1], order.by = fleet_dev_clean$Date)

fleet_dev_daily_list <- list()  # Create an empty list to store the daily time series

for (i in seq(1, ncol(fleet_dev_clean))) {
  macro_var <- fleet_dev_clean[, i]
  header <- colnames(fleet_dev_clean)[i]  # Get the header corresponding to the column
  macro_var_daily <- td(macro_var ~ 1, conversion = "mean", method = "chow-lin-maxlog", to = "day")
  macro_var_daily <- setNames(macro_var_daily$values, header)  # Set the name of the time series to the header
  fleet_dev_daily_list[[header]] <- macro_var_daily  # Add the time series to the list with the header as the name
}

fleet_dev_daily_df <- data.frame(cbind(fleet_dev_daily_list$`Handysize Bulkcarrier Fleet Development [No]`,
                                       fleet_dev_daily_list$`Handymax Bulkcarrier Fleet Development [No]`,
                                       fleet_dev_daily_list$`Panamax Bulkcarrier Fleet Development [No]`,
                                       fleet_dev_daily_list$`Capesize Bulkcarrier Fleet Development [No]`,
                                       fleet_dev_daily_list$`Handysize Bulkcarrier Fleet Development [DWT million]`,
                                       fleet_dev_daily_list$`Handymax Bulkcarrier Fleet Development [DWT million]`,
                                       fleet_dev_daily_list$`Panamax Bulkcarrier Fleet Development [DWT million]`,
                                       fleet_dev_daily_list$`Capesize Bulkcarrier Fleet Development [DWT million]`))

rownames(fleet_dev_daily_df) <- format(as.Date(rownames(fleet_dev_daily_df)), "%d-%m-%Y")

write.csv(fleet_dev_daily_df, "./data/other/fleet_dev_daily.csv", row.names = TRUE)


# ORDERBOOK
orderbook_clean <- macro_data_cleaned[[3]]
orderbook_clean$Date <- as.Date(orderbook_clean$Date)
orderbook_clean <- xts(orderbook_clean[,-1], order.by = orderbook_clean$Date)

orderbook_daily_list <- list()  # Create an empty list to store the daily time series

for (i in seq(1, ncol(orderbook_clean))) {
  macro_var <- orderbook_clean[, i]
  header <- colnames(orderbook_clean)[i]  # Get the header corresponding to the column
  macro_var_daily <- td(macro_var ~ 1, conversion = "mean", method = "chow-lin-maxlog", to = "day")
  macro_var_daily <- setNames(macro_var_daily$values, header)  # Set the name of the time series to the header
  orderbook_daily_list[[header]] <- macro_var_daily  # Add the time series to the list with the header as the name
}

orderbook_daily_df <- data.frame(cbind(orderbook_daily_list$`Handysize Bulker Orderbook [No]`,
                                       orderbook_daily_list$`Handymax Bulker Orderbook [No]`,
                                       orderbook_daily_list$`Panamax Bulker Orderbook [No]`,
                                       orderbook_daily_list$`Capesize Bulker Orderbook [No]`,
                                       orderbook_daily_list$`Handysize Bulker Orderbook [CGT]`,
                                       orderbook_daily_list$`Handymax Bulker Orderbook [CGT]`,
                                       orderbook_daily_list$`Panamax Bulker Orderbook [CGT]`,
                                       orderbook_daily_list$`Capesize Bulker Orderbook [CGT]`,
                                       orderbook_daily_list$`Handysize Bulker Orderbook [DWT]`,
                                       orderbook_daily_list$`Handymax Bulker Orderbook [DWT]`,
                                       orderbook_daily_list$`Capesize Bulker Orderbook [DWT]`,
                                       orderbook_daily_list$`Handysize Bulker Orderbook [GT]`,
                                       orderbook_daily_list$`Handymax Bulker Orderbook [GT]`,
                                       orderbook_daily_list$`Capesize Bulker Orderbook [GT]`,
                                       orderbook_daily_list$`Panamax Bulker Orderbook [DWT]`))

rownames(orderbook_daily_df) <- format(as.Date(rownames(orderbook_daily_df)), "%d-%m-%Y")

write.csv(orderbook_daily_df, "./data/other/orderbook_daily.csv", row.names = TRUE)


# STEEL PRODUCTION
steel_prod_clean <- macro_data_cleaned[[4]]
steel_prod_clean <- steel_prod_clean[,-3]
steel_prod_clean$Date <- as.Date(steel_prod_clean$Date)
steel_prod_clean <- xts(steel_prod_clean[,-1], order.by = steel_prod_clean$Date)

steel_prod_daily_list <- list()  # Create an empty list to store the daily time series

for (i in seq(1, ncol(steel_prod_clean))) {
  macro_var <- steel_prod_clean[, i]
  header <- colnames(steel_prod_clean)[i]  # Get the header corresponding to the column
  macro_var_daily <- td(macro_var ~ 1, conversion = "sum", method = "chow-lin-maxlog", to = "day")
  macro_var_daily <- setNames(macro_var_daily$values, header)  # Set the name of the time series to the header
  steel_prod_daily_list[[header]] <- macro_var_daily  # Add the time series to the list with the header as the name
}

steel_prod_daily_df <- data.frame(cbind(steel_prod_daily_list$`World Steel Production [000 tonnes]`))

rownames(steel_prod_daily_df) <- format(as.Date(rownames(steel_prod_daily_df)), "%d-%m-%Y")

write.csv(steel_prod_daily_df, "./data/other/steel_prod_daily.csv", row.names = TRUE)


# VESSEL SALE VOLUME
vessel_sale_clean <- macro_data_cleaned[[5]]
vessel_sale_clean$Date <- as.Date(vessel_sale_clean$Date)
vessel_sale_clean <- xts(vessel_sale_clean[,-1], order.by = vessel_sale_clean$Date)

vessel_sale_daily_list <- list()  # Create an empty list to store the daily time series

for (i in seq(1, ncol(vessel_sale_clean))) {
  macro_var <- vessel_sale_clean[, i]
  header <- colnames(vessel_sale_clean)[i]  # Get the header corresponding to the column
  macro_var_daily <- td(macro_var ~ 1, conversion = "sum", method = "chow-lin-maxlog", to = "day")
  macro_var_daily <- setNames(macro_var_daily$values, header)  # Set the name of the time series to the header
  vessel_sale_daily_list[[header]] <- macro_var_daily  # Add the time series to the list with the header as the name
}

vessel_sale_daily_df <- data.frame(cbind(vessel_sale_daily_list$`Handysize Bulker Sales [No]`,
                                         vessel_sale_daily_list$`Handymax Bulker Sales [No]`,
                                         vessel_sale_daily_list$`Panamax Bulker Sales [No]`,
                                         vessel_sale_daily_list$`Capesize Sales [No]`,
                                         vessel_sale_daily_list$`Handysize Bulker Sales [DWT]`,
                                         vessel_sale_daily_list$`Handymax Bulker Sales [DWT]`,
                                         vessel_sale_daily_list$`Panamax Bulker Sales [DWT]`,
                                         vessel_sale_daily_list$`Capesize Sales [DWT]`,
                                         vessel_sale_daily_list$`Handysize Bulker Sales [$m]`,
                                         vessel_sale_daily_list$`Handymax Bulker Sales [$m]`,
                                         vessel_sale_daily_list$`Panamax Bulker Sales [$m]`,
                                         vessel_sale_daily_list$`Capesize Sales [$m]`,
                                         vessel_sale_daily_list$`Handysize Bulker Sales [GT]`,
                                         vessel_sale_daily_list$`Handymax Bulker Sales [GT]`,
                                         vessel_sale_daily_list$`Panamax Bulker Sales [GT]`,
                                         vessel_sale_daily_list$`Capesize Sales [GT]`))

rownames(vessel_sale_daily_df) <- format(as.Date(rownames(vessel_sale_daily_df)), "%d-%m-%Y")

write.csv(vessel_sale_daily_df, "./data/other/vessel_sale_daily.csv", row.names = TRUE)


# GLOBAL BULK TRADE INDICATOR
gbti_clean <- gbti_dev[complete.cases(gbti_dev$Date), ]
start_date <- as.Date(train_lev$Date[1])
gbti_clean <- gbti_clean[gbti_clean$Date >= start_date, ]
gbti_clean <- xts(gbti_clean[, -1], order.by = gbti_clean$Date)

gbti_daily_list <- list()

for (i in seq(1, ncol(gbti_clean))) {
  macro_var <- gbti_clean[, i]
  header <- colnames(gbti_clean)[i]  # Get the header corresponding to the column
  macro_var_daily <- td(macro_var ~ 1, conversion = "mean", method = "chow-lin-maxlog", to = "day")
  macro_var_daily <- setNames(macro_var_daily$values, header)  # Set the name of the time series to the header
  gbti_daily_list[[header]] <- macro_var_daily  # Add the time series to the list with the header as the name
}

gbti_daily_df <- data.frame(cbind(gbti_daily_list$`Monthly Global Seaborne Iron Ore Trade Indicator [Volume Index]`,
                                  gbti_daily_list$`Monthly Global Seaborne Coal Trade Indicator [Volume Index]`,
                                  gbti_daily_list$`Monthly Global Seaborne Grain Trade Indicator [Volume Index]`,
                                  gbti_daily_list$`Monthly Global Seaborne Minor Bulk Trade Indicator [Volume Index]`,
                                  gbti_daily_list$`Monthly Global Seaborne Dry Bulk Trade Indicator [Volume Index]`,
                                  gbti_daily_list$`Monthly Global Seaborne Iron Ore Trade Indicator [% Yr/Yr]`,
                                  gbti_daily_list$`Monthly Global Seaborne Iron Ore Trade Indicator [% Yr/Yr 3mma]`,
                                  gbti_daily_list$`Monthly Global Seaborne Coal Trade Indicator [% Yr/Yr]`,
                                  gbti_daily_list$`Monthly Global Seaborne Coal Trade Indicator [% Yr/Yr 3mma]`,
                                  gbti_daily_list$`Monthly Global Seaborne Grain Trade Indicator [% Yr/Yr]`,
                                  gbti_daily_list$`Monthly Global Seaborne Grain Trade Indicator [% Yr/Yr 3mma]`,
                                  gbti_daily_list$`Monthly Global Seaborne Minor Bulk Trade Indicator [% Yr/Yr]`,
                                  gbti_daily_list$`Monthly Global Seaborne Minor Bulk Trade Indicator [% Yr/Yr 3mma]`,
                                  gbti_daily_list$`Monthly Global Seaborne Dry Bulk Trade Indicator [% Yr/Yr]`,
                                  gbti_daily_list$`Monthly Global Seaborne Dry Bulk Trade Indicator [% Yr/Yr 3mma]`))

rownames(gbti_daily_df) <- format(as.Date(rownames(gbti_daily_df)), "%d-%m-%Y")

write.csv(gbti_daily_df, "./data/other/gbti_daily.csv", row.names = TRUE)


# OECD IP
oecd_clean <- oecd_ip_dev[complete.cases(oecd_ip_dev$Date), ]
start_date <- as.Date(train_lev$Date[1])
oecd_clean$Date <- floor_date(oecd_clean$Date, "month")
oecd_clean <- oecd_clean[oecd_clean$Date >= start_date, ]
oecd_clean <- xts(oecd_clean[, -1], order.by = oecd_clean$Date)

oecd_daily_list <- list()

for (i in seq(1, ncol(oecd_clean))) {
  macro_var <- oecd_clean[, i]
  header <- colnames(oecd_clean)[i]  # Get the header corresponding to the column
  macro_var_daily <- td(macro_var ~ 1, conversion = "mean", method = "chow-lin-maxlog", to = "day")
  macro_var_daily <- setNames(macro_var_daily$values, header)  # Set the name of the time series to the header
  oecd_daily_list[[header]] <- macro_var_daily  # Add the time series to the list with the header as the name
}

oecd_daily_df <- data.frame(cbind(oecd_daily_list$`OC PRODUCTION - TOTAL INDUSTRY EXCL. CONSTRUCTION VOLA`,
                                  oecd_daily_list$`OC PRODUCTION - TOTAL INDUSTRY EXCL. CONSTRUCTION SADJ`))

rownames(oecd_daily_df) <- format(as.Date(rownames(oecd_daily_df)), "%d-%m-%Y")

write.csv(oecd_daily_df, "./data/other/oecd_daily.csv", row.names = TRUE)




# Step 3: STATIONARITY CHECKS

# Perform Augmented Dickey-Fuller Test
lapply(train_lev_ts, function(series) adf.test(series, alternative = "stationary"))
lapply(train_diff_ts, function(series) adf.test(series, alternative = "stationary"))





# Step 4: MODEL FITS AND DIAGNOSTIC CHECKS

# VAR
# Select an appropriate lag order, p. You can use the VARselect function for guidance
lags <- VARselect(train_diff_ts, type = "const")
lags
lag_order <- VARselect(train_diff_ts, type = "both")$selection["AIC(n)"]
var_model <- VAR(train_diff_ts, p = lag_order, type = "both")
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
#var_irf_spot_to_spot <- irf(var_model, impulse = "spot", response = "spot")
#plot(var_irf_spot_to_spot, main = "Shock from 'spot' to 'spot'")

# 2. Shock from "spot" to "forwp"
#var_irf_spot_to_forwp <- irf(var_model, impulse = "spot", response = "forwp")
#plot(var_irf_spot_to_forwp, main = "Shock from 'spot' to 'forwp'")

# 3. Shock from "spot" to "forw1m"
#var_irf_spot_to_forw1m <- irf(var_model, impulse = "spot", response = "forw1m")
#plot(var_irf_spot_to_forw1m, main = "Shock from 'spot' to 'forw1m'")

# 4. Shock from "forwp" to "spot"
#var_irf_forwp_to_spot <- irf(var_model, impulse = "forwp", response = "spot")
#plot(var_irf_forwp_to_spot, main = "Shock from 'forwp' to 'spot'")

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
forecast_horizon <- 10
#vecm_forecasts <- predict(vecm_model, n.ahead = forecast_horizon)
var_fcs <- predict(var_model, n.ahead = forecast_horizon) 
vecm_var <- vec2var(coint_test)
vecm_fcs <- predict(vecm_var, n.ahead = forecast_horizon)

#n_forecast <- nrow(test_log_levels)  # Number of points to forecast equals the size of the test set

arima_fcs_spot <- forecast(arima_model_spot, h = forecast_horizon)
arima_fcs_forwp <- forecast(arima_model_forwp, h = forecast_horizon)

# Determine the number of rounds based on the test set size and forecast horizon
num_rounds <- min(floor(nrow(test_lev) / forecast_horizon), 50)
#num_rounds <- floor(nrow(test_lev) / forecast_horizon)

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
  VECM_forwp = numeric()
)

mae_results <- list(
  VAR_spot = numeric(),
  VAR_forwp = numeric(),
  ARIMA_spot = numeric(),
  ARIMA_forwp = numeric(),
  RW_spot = numeric(),
  RW_forwp = numeric(),
  VECM_spot = numeric(),
  VECM_forwp = numeric()
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
  VECM_forwp = numeric()
)

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
  
  
  ## VAR
  lag_order <- VARselect(train_diff_ts, type = "both")$selection["AIC(n)"]
  var_model <- VAR(train_diff_ts, p = lag_order, type = "both")
  var_fcs <- predict(var_model, n.ahead = forecast_horizon)  
  
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
  
  # Calculate and append MAE for VECM forecasts
  mae_results$VECM_spot <- c(mae_results$VECM_spot, mean(abs(vecm_fcs$fcst[[1]][, "fcst"] - act_spot)))
  mae_results$VECM_forwp <- c(mae_results$VECM_forwp, mean(abs(vecm_fcs$fcst[[2]][, "fcst"] - act_forwp)))
  
  # Calculate and append MAE for ARIMA forecasts
  mae_results$ARIMA_spot <- c(mae_results$ARIMA_spot, mean(abs(arima_fcs_spot$mean - act_spot)))
  mae_results$ARIMA_forwp <- c(mae_results$ARIMA_forwp, mean(abs(arima_fcs_forwp$mean - act_forwp)))
  
  # Calculate and append MAE for Random Walk forecasts
  mae_results$RW_spot <- c(mae_results$RW_spot, mean(abs(rw_fcs_spot$mean - act_spot)))
  mae_results$RW_forwp <- c(mae_results$RW_forwp, mean(abs(rw_fcs_forwp$mean - act_forwp)))
  
  
  
}

# Calculate the average direction accuracy for each model and market (spot and forwp)
average_direction_accuracy <- list()
for (model_name in names(direction_accuracy_results)) {
  average_direction_accuracy[[model_name]] <- mean(direction_accuracy_results[[model_name]]) * 100
}

# Print the average direction accuracy for each model
print("Average Direction Accuracy for Each Model (%):")
print(average_direction_accuracy)


# After all rounds are complete, calculate % RMSE reduction from RW
for (model_name in model_names) {
  if (model_name != "RW_spot" && model_name != "RW_forwp") {
    rmse_reduction_results[[model_name]] <- (1 - mean(unlist(rmse_results[[model_name]])) / mean(unlist(rmse_results$RW_spot))) * 100
  }
}

# Calculate mean MSE for each model
mean_rmse_results <- sapply(rmse_results, mean)
mean_mae_results <- sapply(mae_results, mean)
# Now multiply all mean MSE results by 100
mean_mse_results <- mean_mse_results * 100

# Print mean MSE results
cat("Mean RMSE Results:\n")
print(mean_rmse_results)

# Print mean MAE results
cat("Mean MAE Results:\n")
print(mean_mae_results)

rmse_reduction_from_rw <- list(
  VAR_spot = numeric(),
  VAR_forwp = numeric(),
  ARIMA_spot = numeric(),
  ARIMA_forwp = numeric(),
  RW_spot = numeric(),
  RW_forwp = numeric(),
  VECM_spot = numeric(),
  VECM_forwp = numeric()
)

# Calculate % RMSE Reduction for each model from RW
for(model_name in names(mean_rmse_results)) {
  if(model_name != "RW_spot" && model_name != "RW_forwp") { # Exclude RW itself
    rmse_reduction_from_rw[[model_name]] <- (1 - mean_rmse_results[[model_name]] / mean_rmse_results[["RW_spot"]]) * 100
  }
}

# Print results
cat("RW redecution:\n")
print(rmse_reduction_from_rw)

