import pandas as pd
import numpy as np
import math
import json
from keras.models import Sequential
from keras.layers import LSTM, Dense, Input
import pandas as pd
from sklearn.preprocessing import MinMaxScaler, StandardScaler
from sklearn.metrics import mean_squared_error, mean_absolute_error
from concurrent.futures import ProcessPoolExecutor
from collections import defaultdict
from statsmodels.stats.weightstats import CompareMeans

def calculate_mape(y_true, y_pred):
    return np.mean(np.abs((y_true - y_pred) / y_true)) * 100

def direction_accuracy(actual, forecast):
    actual_direction = np.sign(np.diff(actual))
    forecast_direction = np.sign(np.diff(forecast))
    correct = np.sum(actual_direction == forecast_direction)
    return correct / len(actual_direction) * 100

def rmse_reduction(rmse_model, rmse_rw):
    return (1 - rmse_model / rmse_rw) * 100

def diebold_mariano_test(actual, pred1, pred2):
    # pred1 is the predictions from the model being tested
    # pred2 is the predictions from the comparison model (RW)
    d1 = actual - pred1
    d2 = actual - pred2
    dm_stat, p_value = CompareMeans.from_data(d1, d2).ztest_ind(usevar='unequal')
    return dm_stat, p_value

def calculate_rmse(y_true, y_pred):
    return np.sqrt(mean_squared_error(y_true, y_pred))

def calculate_mae(y_true, y_pred):
    return mean_absolute_error(y_true, y_pred)

def calculate_corr_dir(actual_values, predicted_values, last_value):
    sign_act = sign_act = np.sign(actual_values.iloc[-1] - last_value)
    sign_pred = np.sign(predicted_values[-1] - last_value)
    if sign_act == sign_pred:
        return 1
    else:
        return 0

def calculate_avg_dir_accuracy(values):
    return sum(values) / len(values) * 100


def calculate_rmse_reduction(baseline_rmse, model_rmse):
    return (baseline_rmse - model_rmse) / baseline_rmse * 100



def random_walk_predictions(training_data, testing_data):
    """
    Generates Random Walk predictions where the next value is assumed to be the last observed value.
    
    Parameters:
    - training_data: DataFrame containing the training data.
    - testing_data: DataFrame containing the test data.
    
    Returns:
    - predictions: Numpy array containing Random Walk predictions for the test set.
    """
    # Last observed values from the training set
    last_observed_spot = training_data['spot'].iloc[-1]
    last_observed_forwp = training_data['forwp'].iloc[-1]
    
    # Create an array of predictions, each one equal to the last observed values
    predictions = np.tile([last_observed_spot, last_observed_forwp], (len(testing_data), 1))
    
    return predictions


def create_even_odd_array(arr):
    """
    Returns an array where the first column contains values from even positions
    and the second column contains values from odd positions of the original array.
    """
    return arr.reshape(-1, 2)


def create_dataset(dataset, look_back=10, hor=1, is_test=False, exog_col=None):
    X, Y = [], []
    if is_test:  # for test data, we just need the last entry for 1-step ahead forecast
        X = dataset[-1:,:,]
        return np.array(X), None
    else:
        for i in range(look_back, len(dataset) - hor + 1):
            X.append(dataset[i - look_back:i])
            if exog_col is not None:
                # Exclude specified columns from Y
                y = np.delete(dataset[i:i + hor], exog_col, axis=1)
            else:
                y = dataset[i:i + hor]
            Y.append(y)
    return np.array(X), np.array(Y)


def create_dataset_point(dataset, look_back=10, hor=1, is_test=False, exog_col=None):
    X, Y = [], []
    if is_test:  # for test data, we just need the last entry for 1-step ahead forecast
        X = dataset[-1:,:,:]
        return np.array(X), None
    else:
        for i in range(look_back, len(dataset) - hor + 1):
            X.append(dataset[i - look_back:i])
            if exog_col is not None:
                # Exclude specified columns and take only the point at 'hor' for Y
                y = np.delete(dataset[i + hor - 1: i + hor], exog_col, axis=1)
            else:
                y = dataset[i + hor - 1: i + hor]
            Y.append(y)
    return np.array(X), np.array(Y)


def mlp_forecast(trainX_flat, trainY_flat, testX, scaler, epochs, batch_size, verbose):
    # Create and fit the MLP model
    model_mlp = Sequential()
    model_mlp.add(Input(shape=(trainX_flat.shape[1],)))  # Add Input layer
    model_mlp.add(Dense(32, activation='relu'))
    model_mlp.add(Dense(trainY_flat.shape[1], activation="linear"))
    model_mlp.compile(loss='mean_squared_error', optimizer='adam')
    model_mlp.fit(trainX_flat, trainY_flat, epochs=epochs, batch_size=batch_size, verbose=verbose)


    # Make predictions
    trainPredict_scal_flat = model_mlp.predict(trainX_flat)
    #testX, _ = create_dataset(trainX, look_back=look_back, is_test=True)
    testX_flat = testX.reshape(testX.shape[0], -1)
    testPredict_scal_flat = model_mlp.predict(testX_flat)
    testPredict_scal = create_even_odd_array(testPredict_scal_flat)

    # Invert predictions
    #testPredict_mlp = scaler.inverse_transform(testPredict_scal)
    placeholder_exog = np.zeros((testPredict_scal.shape[0], 1))  # Assuming zero as placeholder
    testPredict_expanded = np.hstack((testPredict_scal, placeholder_exog))
    # Step 2: Inverse transform the expanded array
    testPredict_unscaled = scaler.inverse_transform(testPredict_expanded)
    # Step 3: Extract the original predictions (now inversely scaled)
    testPredict = testPredict_unscaled[:, :2]  # Assuming the first two columns are what you need
    return testPredict

def lstm_forecast(trainX, trainY_flat, testX, scaler, epochs, batch_size, verbose):
        # Define the LSTM model with the Input layer
        model_lstm = Sequential()
        model_lstm.add(Input(shape=(trainX.shape[1], trainX.shape[2])))
        model_lstm.add(LSTM(units=50, return_sequences=True))
        model_lstm.add(LSTM(units=50))
        model_lstm.add(Dense(trainY_flat.shape[1], activation='linear'))

        model_lstm.compile(loss='mean_squared_error', optimizer='adam')
        model_lstm.fit(trainX, trainY_flat, epochs=epochs, batch_size=batch_size, verbose=verbose)



        testPredict_scal_flat = model_lstm.predict(testX)
        testPredict_scal = create_even_odd_array(testPredict_scal_flat)
        
        placeholder_exog = np.zeros((testPredict_scal.shape[0], 1))  # Example placeholder
        testPredict_expanded = np.hstack((testPredict_scal, placeholder_exog))
        testPredict_unscaled = scaler.inverse_transform(testPredict_expanded)

        testPredict = testPredict_unscaled[:, :2]  # Assuming the first two columns are the endogenous variables you predicted
        return testPredict


def mlp_point_forecast(train_scal, look_back, last_train_values, scaler, combined_testPredict_point, hor, epochs, batch_size, verbose):
    for step_ahead in range(1, hor + 1):
        # Create dataset for current horizon
        trainX, trainY = create_dataset_point(train_scal, look_back=look_back, hor=step_ahead, exog_col=[2])

        # Flatten input and output
        trainX_flat = trainX.reshape(trainX.shape[0], -1)
        trainY_flat = trainY.reshape(trainY.shape[0], -1)
                
        # Define and fit the MLP model for the current horizon
        model_mlp_point = Sequential()
        model_mlp_point.add(Input(shape=(trainX_flat.shape[1],)))
        model_mlp_point.add(Dense(32, activation='relu'))
        model_mlp_point.add(Dense(trainY_flat.shape[1], activation="linear"))
        model_mlp_point.compile(loss='mean_squared_error', optimizer='adam')
        model_mlp_point.fit(trainX_flat, trainY_flat, epochs=epochs, batch_size=batch_size, verbose=verbose)

        # Use the original last points to predict the step ahead
        testX_flat = last_train_values.reshape(last_train_values.shape[0], -1)
        testPredict_scal_flat_point = model_mlp_point.predict(testX_flat)
        
        # Store the prediction for the current horizon
        combined_testPredict_point[0, (step_ahead - 1) * 2: step_ahead * 2] = testPredict_scal_flat_point

        # Invert predictions
        testPredict_scal = create_even_odd_array(combined_testPredict_point)
        
        # Invert predictions
        #testPredict_mlp = scaler.inverse_transform(testPredict_scal)
        placeholder_exog = np.zeros((testPredict_scal.shape[0], 1))  # Assuming zero as placeholder
        testPredict_expanded = np.hstack((testPredict_scal, placeholder_exog))
        # Step 2: Inverse transform the expanded array
        testPredict_unscaled = scaler.inverse_transform(testPredict_expanded)
        # Step 3: Extract the original predictions (now inversely scaled)
        testPredict = testPredict_unscaled[:, :2]  # Assuming the first two columns are what you need
        return testPredict



def lstm_point_forecast(train_scal, look_back, last_training_values, scaler, combined_testPredict_point, hor, epochs, batch_size, verbose):
    for step_ahead in range(1, hor + 1):
        # Create dataset for current horizon
        trainX, trainY = create_dataset_point(train_scal, look_back=look_back, hor=step_ahead, exog_col=[2])

        # Flatten input and output
        trainX_flat = trainX.reshape(trainX.shape[0], -1)
        trainY_flat = trainY.reshape(trainY.shape[0], -1)
        
        # Define the LSTM model with the Input layer
        model_lstm_point = Sequential()
        model_lstm_point.add(Input(shape=(trainX.shape[1], trainX.shape[2])))
        model_lstm_point.add(LSTM(units=50, return_sequences=True))
        model_lstm_point.add(LSTM(units=50))
        model_lstm_point.add(Dense(trainY_flat.shape[1], activation='linear'))

        model_lstm_point.compile(loss='mean_squared_error', optimizer='adam')
        model_lstm_point.fit(trainX, trainY_flat, epochs=epochs, batch_size=batch_size, verbose=verbose)


        # Use the original last points to predict the step ahead
        testX = last_training_values
        testPredict_scal_flat_point = model_lstm_point.predict(testX)
        
        # Store the prediction for the current horizon
        combined_testPredict_point[0, (step_ahead - 1) * 2: step_ahead * 2] = testPredict_scal_flat_point

        # Invert predictions
        testPredict_scal = create_even_odd_array(combined_testPredict_point)
        
        # Invert predictions
        #testPredict_mlp = scaler.inverse_transform(testPredict_scal)
        placeholder_exog = np.zeros((testPredict_scal.shape[0], 1))  # Assuming zero as placeholder
        testPredict_expanded = np.hstack((testPredict_scal, placeholder_exog))
        # Step 2: Inverse transform the expanded array
        testPredict_lstm_point_expanded = scaler.inverse_transform(testPredict_expanded)
        # Step 3: Extract the original predictions (now inversely scaled)
        testPredict_lstm_point = testPredict_lstm_point_expanded[:, :2]  # Assuming the first two columns are what you need            
    
def train_and_evaluate(data_log_levels, models, split_index, look_back=10, hor=1, exog_col=[2], epochs=30, batch_size=1, verbose=0):
    # Update train and test sets
    train = data_log_levels.iloc[:split_index]
    test = data_log_levels.iloc[split_index:split_index+hor]

    #Scale train set
    scaler = MinMaxScaler()
    train_scal = scaler.fit_transform(train)
    trainX, trainY = create_dataset(train_scal, look_back=look_back, hor=hor, exog_col=exog_col)
    trainX_flat = trainX.reshape(trainX.shape[0], -1)
    trainY_flat = trainY.reshape(trainY.shape[0], -1)
    
    testX = train_scal[-look_back:].reshape(1, look_back, train_scal.shape[1])

    
    results = {}
    for model in models:
        # Store the original last look_back points to initiate prediction for each model
        last_train_values = train_scal[-look_back:].reshape(1, look_back, train_scal.shape[1])
        # Placeholder to store combined predictions for all horizons
        combined_testPredict_point = np.zeros((1, hor * 2))  # Multiply by 2 as each step predicts 2 variables
        if model == "MLP":
            testPredict = mlp_forecast(trainX_flat=trainX_flat, trainY_flat=trainY_flat, testX=testX, scaler=scaler, epochs=epochs, batch_size=batch_size, verbose=verbose)
        elif model == "LSTM":
            testPredict = lstm_forecast(trainX=trainX, trainY_flat=trainY_flat, testX=testX, scaler=scaler, epochs=epochs, batch_size=batch_size, verbose=verbose)
        elif model == "MLP_point":
            testPredict = mlp_point_forecast(train_scal=train_scal, look_back=look_back, last_training_values=last_train_values, scaler=scaler,
                                             combined_testPredict_point=combined_testPredict_point, hor=hor, epochs=epochs, batch_size=batch_size, verbose=verbose)
        elif model == "LSTM_point":
            testPredict = lstm_point_forecast(train_scal=train_scal, look_back=look_back, last_training_values=last_train_values, scaler=scaler,
                                              combined_testPredict_point=combined_testPredict_point, hor=hor, epochs=epochs, batch_size=batch_size, verbose=verbose)
 
        # metrics
        last_value_spot = train.iloc[-1, 0]
        last_value_forw = train.iloc[-1, 1]
        
        rmse_spot = calculate_rmse(test["spot"], testPredict[:,0])
        rmse_forw = calculate_rmse(test["forwp"], testPredict[:,1])
        mae_spot = calculate_mae(test["spot"], testPredict[:,0])
        mae_forw = calculate_mae(test["forwp"], testPredict[:,1])
        mape_spot = calculate_mape(test["spot"], testPredict[:,0])
        mape_forw = calculate_mape(test["forwp"], testPredict[:,1])
        corr_dir_spot = calculate_corr_dir(test["spot"], testPredict[:,0], last_value_spot)
        corr_dir_forw = calculate_corr_dir(test["forwp"], testPredict[:,1], last_value_forw)
        

 
        
        results[model] = {
            "rmse_spot": rmse_spot,
            "rmse_forw": rmse_forw,
            "mae_spot": mae_spot,
            "mae_forw": mae_forw,
            "mape_spot": mape_spot,
            "mape_forw": mape_forw,
            "correct_direction_spot": corr_dir_spot,
            "correct_direction_forw": corr_dir_forw
        }
        
                          
    testPredict = random_walk_predictions(train, test)
    # metrics
    rmse_spot = calculate_rmse(test["spot"], testPredict[:,0])
    rmse_forw = calculate_rmse(test["forwp"], testPredict[:,1])
    mae_spot = calculate_mae(test["spot"], testPredict[:,0])
    mae_forw = calculate_mae(test["forwp"], testPredict[:,1])
    mape_spot = calculate_mape(test["spot"], testPredict[:,0])
    mape_forw = calculate_mape(test["forwp"], testPredict[:,1]) 

    results["RW"] = {
        "rmse_spot": rmse_spot,
        "rmse_forw": rmse_forw,
        "mae_spot": mae_spot,
        "mae_forw": mae_forw,
        "mape_spot": mape_spot,
        "mape_forw": mape_forw
    }     

    return results

def main():
    
    spot = pd.read_csv('./data/spot/clarkson_data.csv', delimiter=';', parse_dates=['Date'], dayfirst=True)
    pmx_forw = pd.read_csv('./data/ffa/PMAX_FFA.csv', delimiter=';', parse_dates=['Date'], dayfirst=True)
    csz_forw = pd.read_csv('./data/ffa/CSZ_FFA.csv', delimiter=';', parse_dates=['Date'], dayfirst=True)
    smx_forw = pd.read_csv('./data/ffa/SMX_FFA.csv', delimiter=';', parse_dates=['Date'], dayfirst=True)
    #oecd_ip_dev = pd.read_csv('./data/other/oecd_daily.csv', parse_dates=['Date'], dayfirst=True)
    #fleet_dev = pd.read_csv('./data/other/fleet_dev_daily.csv', parse_dates=['Date'], dayfirst=True)
    eur_usd = pd.read_csv('./data/other/EUR_USD_historical.csv', parse_dates=['Date'], delimiter=";", dayfirst=True)
    # Convert 'Last' column to numeric, replacing comma with dot for decimal point
    eur_usd['Last'] = pd.to_numeric(eur_usd['Last'].str.replace(',', '.'), errors='coerce')
    
    def pick_forw(key):
        if key == "PMX":
            return pmx_forw
        elif key == "CSZ":
            return csz_forw
        elif key == "SMX":
            return smx_forw

    
    # Number of rounds based on the test set size and forecast horizon
    #num_rounds =  3  # Adjusted to ensure we don't exceed the test set
    look_back = 10  # Adjust based on your temporal structure@
    exog_col = [2]
    hor = 3
    s_col = "CSZ"
    f_col = "1MON"
    #fleet_col = "CSZ fleet"
    forw = pick_forw(s_col)
    epochs = 2
    batch_size = 32
    verbose = 0
    models = ["MLP", "LSTM", "MLP_POINT", "LSTM_POINT", "RW"]



    # Ensure 'Date' columns are in datetime format for all datasets
    #oecd_ip_dev['Date'] = pd.to_datetime(oecd_ip_dev['Date'])
    #fleet_dev['Date'] = pd.to_datetime(fleet_dev['Date'])
    eur_usd['Date'] = pd.to_datetime(eur_usd['Date'])
    spot['Date'] = pd.to_datetime(spot['Date'])
    pmx_forw['Date'] = pd.to_datetime(pmx_forw['Date'])
    csz_forw['Date'] = pd.to_datetime(csz_forw['Date'])
    smx_forw['Date'] = pd.to_datetime(smx_forw['Date'])


    #prod_col = 'Ind Prod Excl Const VOLA'
    eur_col = 'Last'

    # Merge data frames on the Date column
    data_combined = pd.merge(spot, forw, on='Date')
    #data_combined = pd.merge(data_combined, oecd_ip_dev[['Date', prod_col]], on='Date', how='inner')
    #data_combined = pd.merge(data_combined, fleet_dev[['Date', fleet_col]], on='Date', how='inner')
    data_combined = pd.merge(data_combined, eur_usd[['Date', eur_col]], on='Date', how='inner')


    # Filter out rows where the specified columns contain zeros or NA values
    cols_to_check = [s_col, f_col, eur_col]
    data_combined = data_combined.dropna(subset=cols_to_check)  # Drop rows where NA values are present in the specified columns
    data_combined = data_combined[(data_combined[cols_to_check] != 0).all(axis=1)]  # Drop rows where 0 values are present in the specified columns


    # Remove rows with NA or 0 in specific columns (assuming 'SMX' and '1Q' are column names in 'data_combined')
    #data_combined = data_combined[(data_combined[s_col].notna() & data_combined[s_col] != 0) & (data_combined[f_col].notna() & data_combined[f_col] != 0)]

    # Transform data to log levels
    data_log_levels = pd.DataFrame()
    data_log_levels["spot"] = np.log(data_combined[s_col])
    data_log_levels["forwp"] = np.log(data_combined[f_col])
    #data_log_levels[fleet_col] = np.log(data_combined[fleet_col])
    #data_log_levels[prod_col] = np.log(data_combined[prod_col])
    data_log_levels[eur_col] = np.log(data_combined[eur_col])

    data_log_levels.index = data_combined["Date"]
    
    # Validation 
    split_index = math.floor(len(data_log_levels) * 0.7)
    test_index = math.floor(len(data_log_levels) * 0.8)
    len_test = len(data_log_levels[split_index:test_index])
    num_rounds = math.floor(len_test / hor)
    print("Num rounds:",num_rounds)

    # Test
    #split_index = math.floor(len(data_log_levels) * 0.8)
    #len_test = len(data_log_levels[split_index:])
    #num_rounds = math.floor(len_test / hor)
    #print("Num rounds:",num_rounds)


    num_rounds = min(num_rounds, 2)


    
    spot = pd.read_csv('./data/spot/clarkson_data.csv', delimiter=';', parse_dates=['Date'], dayfirst=True)
    pmx_forw = pd.read_csv('./data/ffa/PMAX_FFA.csv', delimiter=';', parse_dates=['Date'], dayfirst=True)
    csz_forw = pd.read_csv('./data/ffa/CSZ_FFA.csv', delimiter=';', parse_dates=['Date'], dayfirst=True)
    smx_forw = pd.read_csv('./data/ffa/SMX_FFA.csv', delimiter=';', parse_dates=['Date'], dayfirst=True)
    #oecd_ip_dev = pd.read_csv('./data/other/oecd_daily.csv', parse_dates=['Date'], dayfirst=True)
    #fleet_dev = pd.read_csv('./data/other/fleet_dev_daily.csv', parse_dates=['Date'], dayfirst=True)
    eur_usd = pd.read_csv('./data/other/EUR_USD_historical.csv', parse_dates=['Date'], delimiter=";", dayfirst=True)
    # Convert 'Last' column to numeric, replacing comma with dot for decimal point
    eur_usd['Last'] = pd.to_numeric(eur_usd['Last'].str.replace(',', '.'), errors='coerce')

    
    split_indices = []
    split_index = split_index - hor #account for first additioin
    for i in range(num_rounds):
        split_index = split_index +  hor
        split_indices.append(split_index)


    results_list = []
    with ProcessPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(train_and_evaluate, data_log_levels, models, split_idx, look_back, hor, exog_col, epochs, batch_size, verbose) for split_idx in split_indices]
        for future in futures:
            results_list.append(future.result())
        # Initialize a dictionary to aggregate scores
        aggregate_results = defaultdict(lambda: defaultdict(list))

        # Aggregate results
        for result in results_list:
            for model, scores in result.items():
                for score_type, value in scores.items():
                    aggregate_results[score_type][model].append(value)
         
        
        metrics = {}    
        # Compute and print mean scores
        for score_type, scores in aggregate_results.items():
            print(score_type)
            for model, values in scores.items():
                if "corr_dir" in score_type:
                    mean_score = calculate_avg_dir_accuracy(values)
                else:
                    mean_score = sum(values) / len(values)
                if not metrics.get(score_type):
                    metrics[score_type] = {}
                metrics[score_type][model] = mean_score
                #print(f"Mean {score_type} for {model}: {mean_score:.5f}")

        for metric in metrics:
            print (metrics[metric])
            
        rmse_rw_spot = metrics["rmse_spot"]["RW"]
        rmse_rw_forw = metrics["rmse_forw"]["RW"]

                   
        for score_type in metrics:
            if "rmse" in score_type:
                for model in metrics[score_type]:
                    if model != "RW":   
                        if "spot" in score_type:
                            rmse_red = calculate_rmse_reduction(rmse_rw_spot, metrics[score_type][model])
                        else:
                            rmse_red = calculate_rmse_reduction(rmse_rw_forw, metrics[score_type][model])
                        print(f"{score_type} reduction for {model}: {rmse_red:.5f}")

 
            
            
            
if __name__ == "__main__":
    main()