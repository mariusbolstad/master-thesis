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