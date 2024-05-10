#install.packages("MCS")
library("readr")
library("dplyr")
library("MCS")
setwd("./VSCode/master-thesis")

# Read your CSV file (replace 'forecasts.csv' with your file path)
data <- read_csv('mcs/csz_1.csv')

# Identify columns that end with '_res'
residual_columns <- grep("_res", names(data), value = TRUE)

# Reorder these columns into a new data frame
residuals_df <- data %>%
  select(all_of(residual_columns))


# Rename columns that contain `_[x,y]` to `_x_y`
rename_columns <- function(colnames) {
  colnames <- gsub("_\\[(.*),(.*)\\]", "_\\1_\\2", colnames)
  colnames <- gsub("\\[(.*)\\]", "_\\1", colnames)
  colnames <- gsub("\\[\\]$", "", colnames)        # Remove trailing []
  return(colnames)
}
# Apply renaming function to the data frame column names
colnames(residuals_df) <- rename_columns(colnames(residuals_df))




# Display the new data frame
print(residuals_df)

# Extract residual columns for MCS (update with your actual residual column names)
#residuals <- as.matrix(data[, c('Model1_Residual', 'Model2_Residual', 'Model3_Residual')])

# Define a loss function (e.g., mean squared error)
loss_function <- function(x) {x^2}

# Apply the loss function to the residuals
loss_matrix <- apply(residuals_df, 2, loss_function)
loss_matrix <- na.omit(loss_matrix)

# Perform the MCS test
mcs_result <- MCSprocedure(loss_matrix, B = 5000, alpha = 0.05, statistic = "Tmax")

# Print the results
print(mcs_result)
