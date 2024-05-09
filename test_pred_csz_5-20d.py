import pandas as pd
import numpy as np
import math
import json
from keras.models import Sequential
from keras.layers import LSTM, Dense, Input, Dropout
from keras.callbacks import EarlyStopping
import pandas as pd
from sklearn.preprocessing import MinMaxScaler, StandardScaler
from sklearn.metrics import mean_squared_error, mean_absolute_error
from concurrent.futures import ProcessPoolExecutor
from collections import defaultdict
from statsmodels.stats.weightstats import CompareMeans
from keras.regularizers import l2

import os
import csv

import logging
import json

import tensorflow as tf


##### THINGS TO CHNAGE ########

#1. local and system test False
#2. Path
#3. s_col
#4. exog_col
#5. 

local = False
system_test = False
path = "test/csz_5-20d"
spot_path = f"{path}_spot"
forw_path = f"{path}_forw"
pred_path = f"{path}_pred"


if local:
    csv_file_spot = f"{spot_path}.csv"
    csv_file_forw = f"{forw_path}.csv"
    log = f"{path}.log"
    max_workers = 2
else:
    csv_file_spot = f"/storage/users/mariumbo/{spot_path}.csv"
    csv_file_forw = f"/storage/users/mariumbo/{forw_path}.csv"
    log = f"/storage/users/mariumbo/{path}.log"
    max_workers = 16




def log_print_csv_spot(data_dict, config_values):
    # Check if the CSV file already exists to decide whether to write headers
    file_exists = os.path.isfile(csv_file_spot)
    
    data = {**config_values, **data_dict}

    with open(csv_file_spot, "a", newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=data.keys())
        
        if not file_exists:
            writer.writeheader()
        
        writer.writerow(data)
        
def log_print_csv_forw(data_dict, config_values):
    # Check if the CSV file already exists to decide whether to write headers
    file_exists = os.path.isfile(csv_file_forw)
    data = {**config_values, **data_dict}


    with open(csv_file_forw, "a", newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=data.keys())
        
        if not file_exists:
            writer.writeheader()
        
        writer.writerow(data)
        

# Setup logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler(log),
                        logging.StreamHandler()
                    ])
logger = logging.getLogger()

def log_metrics(metrics, ):
    # Logging the metrics in a pretty JSON format
    metrics_json = json.dumps(metrics, indent=4)
    logger.info("Logged Metrics:\n" + metrics_json)


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


def create_dataset(dataset, look_back, hor, is_test=False, exog_col=None):
    X, Y = [], []
    if is_test:  # for test data, we just need the last entry for 1-step ahead forecast
        X = dataset[-1:,:,]
        return np.array(X), None
    else:
        for i in range(look_back, len(dataset) - hor + 1):
            X.append(dataset[i - look_back:i])
            if len(exog_col) != 0:
                # Delete the last n=len(exog_col) columns from Y
                n = len(exog_col)
                y = dataset[i:i + hor, :-n]
            else:
                y = dataset[i:i + hor]
            Y.append(y)
    return np.array(X), np.array(Y)


def create_dataset_point(dataset, exog_col, look_back=10, hor=1, is_test=False, ):
    X, Y = [], []
    if is_test:  # for test data, we just need the last entry for 1-step ahead forecast
        X = dataset[-1:,:,:]
        return np.array(X), None
    else:
        for i in range(look_back, len(dataset) - hor + 1):
            X.append(dataset[i - look_back:i])
            if len(exog_col) != 0:
                # Exclude specified columns and take only the point at 'hor' for Y
                y = np.delete(dataset[i + hor - 1: i + hor], exog_col, axis=1)
            else:
                y = dataset[i + hor - 1: i + hor]
            Y.append(y)
    return np.array(X), np.array(Y)


def invert_predictions(testPredict_scal, scaler, exog_col):
    # Invert predictions
    #testPredict_mlp = scaler.inverse_transform(testPredict_scal)
    if len(exog_col) > 0:
        placeholder_exog = np.zeros((testPredict_scal.shape[0], len(exog_col)))  # Assuming zero as placeholder
        testPredict_expanded = np.hstack((testPredict_scal, placeholder_exog))
        # Step 2: Inverse transform the expanded array
        testPredict_unscaled = scaler.inverse_transform(testPredict_expanded)
        testPredict = testPredict_unscaled[:, :2]  # Assuming the first two columns are what you need

    else:
        testPredict = scaler.inverse_transform(testPredict_scal)
    return testPredict

def mlp_predict(trainX_flat, trainY_flat, nodes, layers, epochs, batch_size, verbose, diff,
                earlystop, dropout, regul):
    callbacks = None
    regularizer = None
    if regul:
        regularizer = l2(regul)
    dropout_rate = 0
    if dropout:
        dropout_rate = dropout
    if earlystop:
        callback = EarlyStopping(monitor='loss', patience=earlystop)
        callbacks = [callback]
    model_mlp = Sequential()
    model_mlp.add(Input(shape=(trainX_flat.shape[1],)))  # Add Input layer
    model_mlp.add(Dense(nodes, activation='relu', kernel_regularizer=regularizer))
    if dropout:
        model_mlp.add(Dropout(dropout_rate))
    if layers == 2:
        model_mlp.add(Dense(nodes, activation='relu', kernel_regularizer=regularizer))
        if dropout:
            model_mlp.add(Dropout(dropout_rate))
    model_mlp.add(Dense(trainY_flat.shape[1], activation="linear"))
    model_mlp.compile(loss='mean_squared_error', optimizer='adam')
    model_mlp.fit(trainX_flat, trainY_flat, epochs=epochs, batch_size=batch_size, verbose=verbose, callbacks=callbacks)
    return model_mlp

def lstm_predict(trainX, trainY_flat, nodes, layers, epochs, batch_size, verbose, diff,
                 earlystop, dropout, regul):
    # Define the LSTM model with the Input layer
    callbacks = None
    regularizer = None
    if regul:
        regularizer = l2(regul)
    dropout_rate = 0
    recurrent_dropout = 0
    if dropout:
        dropout_rate = dropout
        recurrent_dropout = dropout
    if earlystop:
        callback = EarlyStopping(monitor='loss', patience=earlystop)
        callbacks = [callback]    
    model_lstm = Sequential()
    model_lstm.add(Input(shape=(trainX.shape[1], trainX.shape[2])))
    model_lstm.add(LSTM(units=nodes, return_sequences=True, kernel_regularizer=regularizer, dropout=dropout_rate, 
                        recurrent_dropout=recurrent_dropout))
    if layers == 2:
        model_lstm.add(LSTM(units=nodes, kernel_regularizer=regularizer, dropout=dropout_rate, 
                        recurrent_dropout=recurrent_dropout))
    model_lstm.add(Dense(trainY_flat.shape[1], activation='linear'))

    model_lstm.compile(loss='mean_squared_error', optimizer='adam')
    callbacks = None
    model_lstm.fit(trainX, trainY_flat, epochs=epochs, batch_size=batch_size, verbose=verbose, callbacks=callbacks)
    return model_lstm
    
    
def mlp_forecast(trainX_flat, trainY_flat, testX, exog_col, scaler, epochs, batch_size, 
                 verbose, nodes, layers, diff, last_value_spot, last_value_forw, earlystop, dropout, regul):
    # Create and fit the MLP model

    model_mlp = mlp_predict(trainX_flat=trainX_flat, trainY_flat=trainY_flat, nodes=nodes, layers=layers, epochs=epochs, 
                            batch_size=batch_size, verbose=verbose, diff=diff, earlystop=earlystop, dropout=dropout, regul=regul)
    # Make predictions
    trainPredict_scal_flat = model_mlp.predict(trainX_flat)
    #testX, _ = create_dataset(trainX, look_back=look_back, is_test=True)
    testX_flat = testX.reshape(testX.shape[0], -1)
    testPredict_scal_flat = model_mlp.predict(testX_flat)
    testPredict_scal = create_even_odd_array(testPredict_scal_flat)

    testPredict = invert_predictions(testPredict_scal, scaler, exog_col)
    if diff:
        testPredict[:, 0] = last_value_spot + testPredict[:, 0].cumsum(axis=0)
        testPredict[:, 1] = last_value_forw + testPredict[:, 1].cumsum(axis=0)

        
    # Step 3: Extract the original predictions (now inversely scaled)
    return testPredict

def lstm_forecast(trainX, trainY_flat, testX, scaler, exog_col, epochs, batch_size, verbose, nodes, layers, diff, 
                  last_value_spot, last_value_forw, earlystop, dropout, regul):
    model_lstm = lstm_predict(trainX=trainX, trainY_flat=trainY_flat, nodes=nodes, layers=layers, epochs=epochs, 
                              batch_size=batch_size, verbose=verbose, diff=diff, earlystop=earlystop, dropout=dropout, regul=regul)
    testPredict_scal_flat = model_lstm.predict(testX)
    testPredict_scal = create_even_odd_array(testPredict_scal_flat)

    testPredict = invert_predictions(testPredict_scal, scaler, exog_col)
    if diff:
        testPredict[:, 0] = last_value_spot + testPredict[:, 0].cumsum(axis=0)
        testPredict[:, 1] = last_value_forw + testPredict[:, 1].cumsum(axis=0)
        
    return testPredict


def mlp_point_forecast(train_scal, look_back, last_train_values, scaler, exog_col, combined_testPredict_point, hor, epochs, batch_size, verbose, nodes, layers):
    for step_ahead in range(1, hor + 1):
        # Create dataset for current horizon
        trainX, trainY = create_dataset_point(train_scal, look_back=look_back, hor=step_ahead, exog_col=exog_col)

        # Flatten input and output
        trainX_flat = trainX.reshape(trainX.shape[0], -1)
        trainY_flat = trainY.reshape(trainY.shape[0], -1)
        
        model_mlp_point = mlp_predict(trainX_flat=trainX_flat, trainY_flat=trainY_flat, nodes=nodes, layers=layers, epochs=epochs, batch_size=batch_size, verbose=verbose)
        # Use the original last points to predict the step ahead
        testX_flat = last_train_values.reshape(last_train_values.shape[0], -1)
        testPredict_scal_flat_point = model_mlp_point.predict(testX_flat)
        
        # Store the prediction for the current horizon
        combined_testPredict_point[0, (step_ahead - 1) * 2: step_ahead * 2] = testPredict_scal_flat_point

    # Invert predictions
    testPredict_scal = create_even_odd_array(combined_testPredict_point)
    
    testPredict = invert_predictions(testPredict_scal, scaler, exog_col)

    return testPredict



def lstm_point_forecast(train_scal, look_back, last_train_values, scaler, exog_col, combined_testPredict_point, hor, epochs, batch_size, verbose, nodes, layers):
    for step_ahead in range(1, hor + 1):
        # Create dataset for current horizon
        trainX, trainY = create_dataset_point(train_scal, look_back=look_back, hor=step_ahead, exog_col=exog_col)

        # Flatten input and output
        trainX_flat = trainX.reshape(trainX.shape[0], -1)
        trainY_flat = trainY.reshape(trainY.shape[0], -1)
        
        # Define the LSTM model with the Input layer
        model_lstm_point = lstm_predict(trainX=trainX, trainY_flat=trainY_flat, nodes=nodes, layers=layers, epochs=epochs, batch_size=batch_size, verbose=verbose)

        # Use the original last points to predict the step ahead
        testX = last_train_values
        testPredict_scal_flat_point = model_lstm_point.predict(testX)
        
        # Store the prediction for the current horizon
        combined_testPredict_point[0, (step_ahead - 1) * 2: step_ahead * 2] = testPredict_scal_flat_point

    # Invert predictions
    testPredict_scal = create_even_odd_array(combined_testPredict_point)
    
    testPredict = invert_predictions(testPredict_scal, scaler, exog_col)
    return testPredict
        
    
def train_and_evaluate(data_log_levels, models, split_index, look_back, hor, exog_col, epochs, batch_size, 
                       verbose, nodes, layers, diff, earlystop, dropout, regul):
    # Update train and test sets
    train = data_log_levels.iloc[:split_index]
    test = data_log_levels.iloc[split_index:split_index+hor]

    #Scale train set
    if diff:
        scaler = StandardScaler()
        train_diff = train.diff().dropna()
        train_diff_scal = scaler.fit_transform(train_diff)
        trainX, trainY = create_dataset(train_diff_scal, look_back=look_back, hor=hor, exog_col=exog_col)
        testX = train_diff_scal[-look_back:].reshape(1, look_back, train_diff_scal.shape[1])

    else:
        scaler = MinMaxScaler()
        train_scal = scaler.fit_transform(train)
        trainX, trainY = create_dataset(train_scal, look_back=look_back, hor=hor, exog_col=exog_col)
        testX = train_scal[-look_back:].reshape(1, look_back, train_scal.shape[1])

    trainX_flat = trainX.reshape(trainX.shape[0], -1)
    trainY_flat = trainY.reshape(trainY.shape[0], -1)
    
    # metrics
    last_value_spot = train.iloc[-1, 0]
    last_value_forw = train.iloc[-1, 1]

    
    results = {}
    for model in models:
        # Store the original last look_back points to initiate prediction for each model
        #last_train_values = train_scal[-look_back:].reshape(1, look_back, train_scal.shape[1])
        last_train_values = None #not using point
        # Placeholder to store combined predictions for all horizons
        combined_testPredict_point = np.zeros((1, hor * 2))  # Multiply by 2 as each step predicts 2 variables
        if model == "MLP":
            testPredict = mlp_forecast(trainX_flat=trainX_flat, trainY_flat=trainY_flat, testX=testX, scaler=scaler, epochs=epochs, 
                                       batch_size=batch_size, verbose=verbose, exog_col=exog_col, nodes=nodes, layers=layers,
                                       diff=diff, last_value_spot=last_value_spot, last_value_forw=last_value_forw,
                                       earlystop=earlystop, dropout=dropout, regul=regul)
        elif model == "LSTM":
            testPredict = lstm_forecast(trainX=trainX, trainY_flat=trainY_flat, testX=testX, scaler=scaler, exog_col=exog_col, 
                                        epochs=epochs, batch_size=batch_size, verbose=verbose,  nodes=nodes, layers=layers, diff=diff,
                                        last_value_spot=last_value_spot, last_value_forw=last_value_forw,
                                        earlystop=earlystop, dropout=dropout, regul=regul)
        elif model == "MLP_POINT":
            testPredict = mlp_point_forecast(train_scal=train_scal, look_back=look_back, last_train_values=last_train_values, scaler=scaler,
                                             combined_testPredict_point=combined_testPredict_point, hor=hor, epochs=epochs, batch_size=batch_size, verbose=verbose, exog_col=exog_col, nodes=nodes, layers=layers)
        elif model == "LSTM_POINT":
            testPredict = lstm_point_forecast(train_scal=train_scal, look_back=look_back, last_train_values=last_train_values, scaler=scaler, 
                                              exog_col=exog_col, combined_testPredict_point=combined_testPredict_point, hor=hor, epochs=epochs, 
                                              batch_size=batch_size, verbose=verbose, nodes=nodes, layers=layers)
            
        if diff:
            pass
 
        # predictions
        pred_spot = testPredict[:,0]
        pred_forw = testPredict[:, 1]        
        

        
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
            "correct_direction_forw": corr_dir_forw,
            "pred_spot": pred_spot,
            "pred_forw": pred_forw
        }
        
       
        
                          
    testPredict = random_walk_predictions(train, test)
    # metrics
    rmse_spot = calculate_rmse(test["spot"], testPredict[:,0])
    rmse_forw = calculate_rmse(test["forwp"], testPredict[:,1])
    mae_spot = calculate_mae(test["spot"], testPredict[:,0])
    mae_forw = calculate_mae(test["forwp"], testPredict[:,1])
    mape_spot = calculate_mape(test["spot"], testPredict[:,0])
    mape_forw = calculate_mape(test["forwp"], testPredict[:,1]) 
    pred_spot = testPredict[:,0]
    pred_forw = testPredict[:, 1]   

    results["RW"] = {
        "rmse_spot": rmse_spot,
        "rmse_forw": rmse_forw,
        "mae_spot": mae_spot,
        "mae_forw": mae_forw,
        "mape_spot": mape_spot,
        "mape_forw": mape_forw,
        "pred_spot": pred_spot,
        "pred_forw": pred_forw       
    }
    
    results["Actual"] = {
         "pred_spot": test["spot"].values,
         "pred_forw": test["forwp"].values   
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
    sp500 = pd.read_csv("./data/other/sp500.csv", parse_dates=['Date'])
    
    #sofr = pd.read_csv()
    # Convert 'Last' column to numeric, replacing comma with dot for decimal point
    eur_usd['Last'] = pd.to_numeric(eur_usd['Last'].str.replace(',', '.'), errors='coerce')
    sp500['Close'] = pd.to_numeric(sp500['Close'].str.replace(',', ''), errors='coerce')


    def pick_forw(key):
        if key == "PMX":
            return pmx_forw
        elif key == "CSZ":
            return csz_forw
        elif key == "SMX":
            return smx_forw
        
    ###### PARAMS ########
    
    #f_col = "1MON"
    f_col_lst = ["1MON"]
    s_col = "CSZ"
    exog_col_lst = [[], [4], [2,4]]
    #exog_col = [2]
    hors = [10,20]
    #hor = 1
    diff = False
    
    
    #### HYPERPARAMS #######
    nodes = 16
    #batch_sizes = [1,32]
    batch_size = 32
    epochs = 100
    if system_test:
        epochs = 1
    verbose = 1
    look_back = 10
    layers = 2
    #dropout_lst = [None, 0.2, 0.5]
    dropout = None
    #regul_lst = [None, 0.01, 0.1]
    regul = None
    #earlystop_lst = [None, 3, 10]    
    earlystop = None
    ######################

    for f_col in f_col_lst:
        for hor in hors:
            for exog_col in exog_col_lst:
                models = ["MLP", "LSTM", "RW"]
                forw = pick_forw(s_col)
                # Ensure 'Date' columns are in datetime format for all datasets
                #oecd_ip_dev['Date'] = pd.to_datetime(oecd_ip_dev['Date'])
                #fleet_dev['Date'] = pd.to_datetime(fleet_dev['Date'])
                eur_usd['Date'] = pd.to_datetime(eur_usd['Date'])
                sp500['Date'] = pd.to_datetime(sp500['Date'])    
                spot['Date'] = pd.to_datetime(spot['Date'])
                pmx_forw['Date'] = pd.to_datetime(pmx_forw['Date'])
                csz_forw['Date'] = pd.to_datetime(csz_forw['Date'])
                smx_forw['Date'] = pd.to_datetime(smx_forw['Date'])


                #prod_col = 'Ind Prod Excl Const VOLA'
                eur_col = 'Last'
                sp500_col = "Close"
                sofr_col = ""
                bdi_col = 'BDI'

                # Merge data frames on the Date column
                data_combined = pd.merge(spot, forw, on='Date')
                #data_combined = pd.merge(data_combined, oecd_ip_dev[['Date', prod_col]], on='Date', how='inner')
                #data_combined = pd.merge(data_combined, fleet_dev[['Date', fleet_col]], on='Date', how='inner')
                data_combined = pd.merge(data_combined, eur_usd[['Date', eur_col]], on='Date', how='inner')
                data_combined = pd.merge(data_combined, sp500[['Date', sp500_col]], on='Date', how='inner')
                #data_combined = pd.merge(data_combined, sofr[['Date', sofr_col]], on='Date', how='inner')


                # Filter out rows where the specified columns contain zeros or NA values
                cols_to_check = [s_col, f_col, eur_col, sp500_col, bdi_col]
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
                for col in exog_col:
                    if col == 2:
                        data_log_levels[eur_col] = np.log(data_combined[eur_col])
                    elif col == 3:
                        data_log_levels[sp500_col] = np.log(data_combined[sp500_col])
                    elif col == 4:
                        data_log_levels[bdi_col] = np.log(data_combined[bdi_col])

                data_log_levels.index = data_combined["Date"]
                
                # Validation 
                #split_index = math.floor(len(data_log_levels) * 0.7)
                #test_index = math.floor(len(data_log_levels) * 0.8)
                #len_test = len(data_log_levels[split_index:test_index])
                #num_rounds = math.floor(len_test / hor)
                #print("Num rounds:",num_rounds)

                # Test
                split_index = math.floor(len(data_log_levels) * 0.8) + 3
                first_split_index = split_index
                print("Split index: ", split_index)
                len_test = len(data_log_levels[split_index:])
                num_rounds = math.floor(len_test / hor)
                print("Num rounds:",num_rounds)

                if system_test:
                    num_rounds = min(num_rounds, 2)

                split_indices = []
                split_index = split_index - hor #account for first additioin
                for i in range(num_rounds):
                    split_index = split_index +  hor
                    split_indices.append(split_index)

                results_list = []
                predictions_list = []
                
                headers = [
                        "Actual", "RW_pred", f"MLP_pred_{exog_col}", f"LSTM_pred_{exog_col}", "RW_res", f"MLP_res_{exog_col}", f"LSTM_res_{exog_col}"]
                # Creating an empty DataFrame with specified headers
                num_rows = len(data_log_levels[first_split_index:first_split_index+hor*num_rounds])
                preds = pd.DataFrame(columns=headers, index=range(num_rows))
                preds["Actual"] = data_log_levels[first_split_index:first_split_index+hor*num_rounds].values
                preds["RW_pred"] = np.zeros(num_rows)
                preds[f"MLP_pred_{exog_col}"] = np.array(num_rows)
                preds[f"LSTM_pred_{exog_col}"] = np.array(num_rows)
                preds["RW_res"] = np.array(num_rows)
                preds[f"MLP_res_{exog_col}"] = np.array(num_rows)
                preds[f"LSTM_res_{exog_col}"] = np.array(num_rows)
                logger.info(f"Spot: {s_col}. Forw: {f_col}. Lookback: {look_back}. Horizon: {hor}. Exog_Col = {exog_col}. Epochs = {epochs}. Nodes: {nodes} Batchsize: {batch_size}")
                with ProcessPoolExecutor(max_workers=max_workers) as executor:
                    futures = [executor.submit(train_and_evaluate, data_log_levels, models, split_idx, look_back, hor, exog_col, epochs, batch_size, verbose, nodes, layers, diff, earlystop, dropout, regul) for split_idx in split_indices]
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
                            elif "pred" in score_type:
                                mean_score = np.concatenate(values, axis=0)
                                if score_type == "pred_spot":
                                    pred_col = None
                                    res_col = None
                                    if model == "RW":
                                        pred_col = "RW_pred"
                                        res_col = "RW_res"
                                    elif model == "MLP" or model == "LSTM":
                                        pred_col = f"{model}_pred_{exog_col}"
                                        res_col = f"{model}_res_{exog_col}"   
                                    if pred_col:   
                                        predictions = np.concatenate(values)
                                        act = preds["Actual"].values
                                        residuals = predictions - act
                                        
                                        preds[pred_col] = predictions
                                        preds[res_col] = residuals
                            else:
                                mean_score = sum(values) / len(values)
                            if not metrics.get(score_type):
                                metrics[score_type] = {}
                            metrics[score_type][model] = mean_score
                            #print(f"Mean {score_type} for {model}: {mean_score:.5f}")

                    # Compute average scores and organize metrics data
                        metrics_summary = {}
                        for metric, models in aggregate_results.items():
                            metrics_summary[metric] = {}
                            for model, values in models.items():
                                if "pred" not in metric:
                                    average = sum(values) / len(values)
                                    metrics_summary[metric][model] = average
                                else:
                                    pass
                                
                                
                    # compute dm test
                    metrics_summary["dm_teststat_spot"] = {}
                    metrics_summary["dm_pvalue_spot"] = {}
                    metrics_summary["dm_teststat_forw"] = {}
                    metrics_summary["dm_pvalue_forw"] = {}
                    
                    for model in metrics_summary["rmse_spot"]:
                        #spot
                        actuals = metrics["pred_spot"]["Actual"]
                        pred = metrics["pred_spot"][model]
                        rw_pred = metrics["pred_spot"]["RW"]
                        teststat, pvalue = diebold_mariano_test(actuals, pred, rw_pred)
                        metrics_summary["dm_teststat_spot"][model] = teststat
                        metrics_summary["dm_pvalue_spot"][model] = pvalue
                        
                        #forw
                        actuals = metrics["pred_forw"]["Actual"]
                        pred = metrics["pred_forw"][model]
                        rw_pred = metrics["pred_forw"]["RW"]
                        teststat, pvalue = diebold_mariano_test(actuals, pred, rw_pred)
                        metrics_summary["dm_teststat_forw"][model] = teststat
                        metrics_summary["dm_pvalue_forw"][model] = pvalue           
                        
                    

                    # Compute reductions for RMSE compared to RW and integrate directly into metrics_summary
                    if "rmse_spot" in metrics_summary and "rmse_forw" in metrics_summary:
                        metrics_summary["reduction_rmse_spot"] = {}
                        metrics_summary["reduction_rmse_forw"] = {}
                        for model in metrics_summary["rmse_spot"]:
                            if model != "RW":
                                metrics_summary["reduction_rmse_spot"][model] = rmse_reduction(metrics_summary["rmse_spot"][model], metrics_summary["rmse_spot"]["RW"] )
                                metrics_summary["reduction_rmse_forw"][model] = rmse_reduction(metrics_summary["rmse_forw"][model], metrics_summary["rmse_forw"]["RW"] )
                            else:
                                # Set reductions for RW to zero as it is the baseline
                                metrics_summary["reduction_rmse_spot"][model] = 0
                                metrics_summary["reduction_rmse_forw"][model] = 0

                    
                    # Create a DataFrame for configurations
                    config_df = {
                        'Spot': s_col,
                        'Forw': f_col,
                        'Lookback': str(look_back),
                        'Horizon': str(hor),
                        'Exog_Col': str(exog_col),
                        'Epochs': str(epochs),
                        'Batchsize': str(batch_size),
                        'Diff': str(diff),
                        'Dropout': str(dropout),
                        'Regularizer': str(regul),
                        'Earlystop': str(earlystop)
                    }

                    # Log and print all metrics
                    log_metrics(metrics_summary)
                    log_print_csv_spot((metrics_summary["reduction_rmse_spot"]), config_df)
                    log_print_csv_forw((metrics_summary["reduction_rmse_forw"]), config_df)

                    #return metrics_summary

            print(preds)
            if local:
                csv_path = f"{pred_path}_{hor}.csv"
            else:
                csv_path = f"/storage/users/mariumbo/{pred_path}_{hor}.csv"
            preds.to_csv(csv_path)
            
            
            
if __name__ == "__main__":
    main()