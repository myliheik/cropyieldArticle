"""
2021-09-01 MY added normalization
2022-01-02 iterable, returns mean RMSE of ntimes iterated trainings.
2022-03-05 return also all RMSEs

RUN:

Without testing set (makes train/validation split automatically):
python 09-runTCN-article-iterate.py -i dataStack/array_1110-2020.npz \
--epochs 200 --batchsize 128 --learningrate 0.001 --epsilon 0.1

With testing set (region or separate year):
python 09-runTCN-article-iterate.py -i dataStack/array_1110-2018-2019.npz \
-j dataStack/array_1110-2020.npz \
--epochs 200 --batchsize 128 --learningrate 0.001 --epsilon 0.1


NOTE: if you test with a separate year, be sure that training set excludes that year!


"""
import glob
import pandas as pd
import numpy as np
import os.path
from pathlib import Path
import argparse
import textwrap
import math
import time
import csv
from scipy import stats
import utils

#os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3' 
from tensorflow.keras.models import Sequential, save_model, load_model
from tensorflow.keras.layers import Dense, Dropout, SimpleRNN, LSTM
from tensorflow.keras.callbacks import EarlyStopping
from tensorflow.keras.utils import plot_model
from tensorflow.keras.optimizers import Adam

from tcn import TCN, tcn_full_summary

# pip install keras-tcn --user

t = time.localtime()
timeString  = time.strftime("%Y-%m-%d", t)

# EDIT:
# How many times to iterate each data set?
ntimes = 10

# FUNCTIONS:

def doRMSE(residuals):
    return np.sqrt(np.square(residuals).mean())

def temporalConvolutionalNetworks(shape1, shape2):
    print("\nTraining TCN...")

    tcn_layer = TCN(input_shape=(None, shape2), nb_filters = 32, padding = 'causal', kernel_size = 2, 
            nb_stacks=1, dilations = [1, 2, 4, 8, 16], 
            return_sequences=True
           )
    
    # The receptive field tells you how far the model can see in terms of timesteps.
    print('Receptive field size =', tcn_layer.receptive_field)

    model = Sequential([
        tcn_layer,
        Dense(1)
        ])
    

    # Model summary:
    print('\nNetwork architecture:')
    print(model.summary())
    #print(tcn_full_summary(model))
    
    return model



def runModel(model, modelname, Xtrain, ytrain, Xtest, ytest, outputdir, epochs, batchsize, optimizeri, lera, epsiloni, setID, normalizer):

    # monitor validation progress
    early = EarlyStopping(monitor = "val_loss", mode = "min", patience = 10)
    callbacks_list = [early]
    
    if optimizeri == 'adam':
        model.compile(loss = 'mean_squared_error',
                  optimizer = Adam(learning_rate=lera, epsilon = epsiloni),
                  metrics = ['mse'])
    df = []
    
    # iterate training:
    for i in range(ntimes):
        print(f'Iteration {i+1}...')
        history = model.fit(Xtrain, ytrain,
            epochs=epochs,  batch_size=batchsize, verbose=0,
            validation_split = 0.20,
            callbacks = callbacks_list)

        test_predictions = model.predict(Xtest)

        dfResiduals = pd.DataFrame(np.subtract(test_predictions[:, -1, 0], ytest))
        dfResiduals.columns = ['farmfinal']

        # in this case using doys (130-243) (43, 73, 104) with zero-padding:
        june = 43
        july = 73
        august = 104

        #June:
        dfResiduals['farm43'] = np.subtract(test_predictions[:, june, 0], ytest)

        #July:
        dfResiduals['farm73'] = np.subtract(test_predictions[:, july, 0], ytest)

        #August:
        dfResiduals['farm104'] = np.subtract(test_predictions[:, august, 0], ytest)

        df.append(dfResiduals)

    return df
    
    
# HERE STARTS MAIN:

def main(args):
    try:
        if not args.inputfile :
            raise Exception('Missing input filepath argument. Try --help .')

        print(f'\n09-runTCN-article-iterate.py')
        print(f'\nARD data set in: {args.inputfile}')
        
        if 'median' in args.inputfile:
            print('Median as a sole feature...')
            normalizer = 'median'
        else:   
            # EDIT:
            #normalizer = "linear" # or "L1"
            normalizer = "L1"

        ############################# Preprocessing:        
        # read in array data:
        xtrain0 = utils.load_npintensities(args.inputfile)
        # normalize:
        xtrain = utils.normalise3D(xtrain0, normalizer)
        # read in target y:
        ytrain = utils.readTarget(args.inputfile)
        # jos ei anneta test set, niin tehdään split:       
        if not args.testfile:
            print(f"\nSplitting {args.inputfile} into validation and training set:")
            xtrain, ytrain, xval, yval = utils.split_data(xtrain, ytrain)
            setID = utils.parse_xpath(args.inputfile)
        else:
            xval0 = utils.load_npintensities(args.testfile)
            # normalize:
            xval = utils.normalise3D(xval0, normalizer)
            yval = utils.readTarget(args.testfile)
            setID = utils.parse_xpath(args.testfile) 
          
        
        if not args.testfile:
            basepath = args.inputfile.split('/')[:-2]
            out_dir_results = os.path.join(os.path.sep, *basepath, 'predictions', timeString + '-iterative')
            Path(out_dir_results).mkdir(parents=True, exist_ok=True)
        else:
            basepath = args.testfile.split('/')[:-2]
            out_dir_results = os.path.join(os.path.sep, *basepath, 'predictions', timeString + '-iterative')
            Path(out_dir_results).mkdir(parents=True, exist_ok=True)
            
        
        # this needs 3D:
        m,n = xtrain.shape[:2]
        xtrain3d = xtrain.reshape(m,n,-1) 
        m,n = xval.shape[:2]
        xval3d = xval.reshape(m,n,-1) 

        # forget zero-padding:
        #if xval3d.shape[1] < xtrain3d.shape[1]:
        #    doysToAdd = xtrain3d.shape[1] - xval3d.shape[1]
        #    print(f"Shape of testing set differs from training set. We need to pad it with {doysToAdd} DOYs.")
        #    b = np.zeros( (xval3d.shape[0],doysToAdd,xval3d.shape[2]) )
        #    xval3d = np.column_stack((xval3d,b))
        #    print(f'New shape of padded xval3d is {xval3d.shape}.')   
            
        #if xtrain3d.shape[1] < xval3d.shape[1]:
        #    doysToAdd = xval3d.shape[1] - xtrain3d.shape[1]
        #    print(f"Shape of training set differs from testing set. We need to pad it with {doysToAdd} DOYs.")
        #    b = np.zeros( (xtrain3d.shape[0],doysToAdd,xtrain3d.shape[2]) )
        #    xtrain3d = np.column_stack((xtrain3d,b))
        #    print(f'New shape of padded xtrain3d is {xtrain3d.shape}.')   

        ##################################### Models:    
        # model topology:      
        model = temporalConvolutionalNetworks(xtrain3d.shape[1], xtrain3d.shape[2])
        if normalizer == 'median':
            modelname = 'TCNmedian'
        else:
            if not args.testfile:
                modelname = 'TCN'
            else:
                modelname = 'TCNtest'
                
        df = runModel(model, modelname, xtrain3d, ytrain, xval3d, yval, out_dir_results, args.epochs, args.batchsize, args.optimizer, args.learningrate, args.epsilon, setID, normalizer)
 
        basepath = args.inputfile.split('/')[:-2]
        out_dir_results = os.path.join(os.path.sep, *basepath, 'predictions', timeString + '-iterative')
        Path(out_dir_results).mkdir(parents=True, exist_ok=True)

        t = time.localtime()
        timeString2  = time.strftime("%Y-%m-%d-%H:%M:%S", t)
        
        pklfile = os.path.join(os.path.sep, *basepath, 'predictions', timeString + '-iterative', timeString2 + '-allIteratedRMSE-' + modelname + '-' + setID + '.pkl')
        
        print(f"\nWriting results to file {pklfile}.")
        utils.save_intensities(pklfile, df)
        
        csvfile = os.path.join(os.path.sep, *basepath, 'predictions', timeString + '-iterative', 'iteratedRMSE.csv')
        print(f"\nWriting results to file {csvfile}.")


        for setti in ['farmfinal', 'farm43', 'farm73', 'farm104']:
            residuals = []
            for i in range(ntimes):
                residuals.extend(df[i][setti])
            rmse = doRMSE(residuals)

            with open(csvfile, "a+") as f:
                writer = csv.writer(f)
                writer.writerow([setID, modelname, round(rmse, 3), setti])
            

        print(f'\nDone.')

    except Exception as e:
        print('\n\nUnable to read input or write out statistics. Check prerequisites and see exception output below.')
        parser.print_help()
        raise e


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                     epilog=textwrap.dedent(__doc__))

    parser.add_argument('-i', '--inputfile',
                        help='Filepath of array intensities (training set).',
                        type=str)
    parser.add_argument('-j', '--testfile',
                        help='Filepath of the testing set (optional).',
                        type=str)   
    parser.add_argument('-e', '--epochs',
                        help='An epoch is an iteration over the entire x and y data provided (default 20).',
                        type=int, default = 20)      
    parser.add_argument('-b', '--batchsize',
                        help='Number of samples per gradient update (default 32).',
                        type=int, default = 32)  
    parser.add_argument('-o', '--optimizer',
                        help='Optimizer (default adam).',
                        type=str, default = 'adam') 
    parser.add_argument('-l', '--learningrate',
                        help='Learning rate (defaults to 0.001).',
                        type=float, default = '0.001') 
    parser.add_argument('-p', '--epsilon',
                        help='A small constant for numerical stability (defaults to 1e-07).',
                        type=float, default = '0.0000001') 
    parser.add_argument('--debug',
                        help='Verbose output for debugging.',
                        action='store_true')

    args = parser.parse_args()
    main(args)
