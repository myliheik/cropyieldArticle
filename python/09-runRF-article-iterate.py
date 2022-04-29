"""
2021-11-30 RF / iterable 2.1.2022

RUN:

Without testing set (makes train/validation split automatically):
python 09-runRF-article-iterate.py -i /Users/myliheik/Documents/myCROPYIELD/scratch/project_2001253/cropyieldII/cloudy/dataStack/array_1110-2020.npz 

With testing set (kunta or separate year):
python 09-runRF-article-iterate.py -i /Users/myliheik/Documents/myCROPYIELD/scratch/project_2001253/cropyieldIII/cloudless/dataStack/array_1110-2018-2019.npz \
-j /Users/myliheik/Documents/myCROPYIELD/scratch/project_2001253/cropyieldIII/cloudless/dataStack/array_1110-2020.npz 


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
import seaborn as sns
import utils

from sklearn.ensemble import RandomForestRegressor
from sklearn import metrics
import matplotlib.pyplot as plt

# EDIT:
# How many times to iterate each data set?
ntimes = 10


t = time.localtime()
timeString  = time.strftime("%Y-%m-%d", t)

# FUNCTIONS:

def runModel(model, Xtrain, ytrain, Xtest):
    model.fit(Xtrain, ytrain)
    test_predictions = model.predict(Xtest)
    return test_predictions

def doRMSE(residuals):
    return np.sqrt(np.square(residuals).mean())

# HERE STARTS MAIN:

def main(args):
    try:
        if not args.inputfile :
            raise Exception('Missing input filepath argument. Try --help .')

        print(f'\n09-runRF-article-iterate.py')
        print(f'\nARD data set in: {args.inputfile}')
        

        
        if 'median' in args.inputfile:
            print('Median as a sole feature')
            normalizer = 'median'
        else:   
            # EDIT:
            #normalizer = "linear" # or "L1"
            normalizer = "L1"
        
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
        
        # this needs 3D:
        m,n = xtrain.shape[:2]
        xtrain3d = xtrain.reshape(m,n,-1) 
        m,n = xval.shape[:2]
        xval3d = xval.reshape(m,n,-1) 

        if xval3d.shape[1] < xtrain3d.shape[1]:
            doysToAdd = xtrain3d.shape[1] - xval3d.shape[1]
            print(f"Shape of testing set differs from training set. We need to pad it with {doysToAdd} DOYs.")
            b = np.zeros( (xval3d.shape[0],doysToAdd,xval3d.shape[2]) )
            xval3d = np.column_stack((xval3d,b))
            print(f'New shape of padded xval3d is {xval3d.shape}.')   
            
        if xtrain3d.shape[1] < xval3d.shape[1]:
            doysToAdd = xval3d.shape[1] - xtrain3d.shape[1]
            print(f"Shape of training set differs from testing set. We need to pad it with {doysToAdd} DOYs.")
            b = np.zeros( (xtrain3d.shape[0],doysToAdd,xtrain3d.shape[2]) )
            xtrain3d = np.column_stack((xtrain3d,b))
            print(f'New shape of padded xtrain3d is {xtrain3d.shape}.')   
        
        # 2D:
        # make 2D:
        m = xval3d.shape[0]
        xval2d = xval3d.reshape(m,-1)
        m = xtrain3d.shape[0]
        xtrain2d = xtrain3d.reshape(m,-1)
        
        #pitää tehdä se in-season ennen kuin 2D:
        june = 43
        july = 73
        august = 104    
        # June:
        xtrain3dnew = xtrain3d[:,:june,:]
        xval3dnew = xval3d[:,:june,:]

        # make 2D:
        m = xval3dnew.shape[0]
        XtestJune= xval3dnew.reshape(m,-1)
        m = xtrain3dnew.shape[0]
        XtrainJune = xtrain3dnew.reshape(m,-1)

        # July:
        xtrain3dnew = xtrain3d[:,:july,:]
        xval3dnew = xval3d[:,:july,:]

        # make 2D:
        m = xval3dnew.shape[0]
        XtestJuly = xval3dnew.reshape(m,-1)
        m = xtrain3dnew.shape[0]
        XtrainJuly = xtrain3dnew.reshape(m,-1)

        # August:
        xtrain3dnew = xtrain3d[:,:august,:]
        xval3dnew = xval3d[:,:august,:]

        # make 2D:
        m = xval3dnew.shape[0]
        XtestAugust = xval3dnew.reshape(m,-1)
        m = xtrain3dnew.shape[0]
        XtrainAugust = xtrain3dnew.reshape(m,-1)
        
        
        # MODEL:
        model = RandomForestRegressor(max_features = 8, n_jobs = -1, n_estimators = 500)

        if normalizer == 'median':
            modelname = 'RFmedian'
        else:
            if not args.testfile:
                modelname = 'RF'
            else:
                if 'ely' in args.testfile:
                    modelname = 'RFely'
                if 'Rank' in args.testfile:
                    modelname = 'RFrank'
                else:
                    modelname = 'RFtest'
                    
        df = []

        # iterate predictions:
        for i in range(ntimes):
            print(f'Iteration {i+1}...')
            test_predictions = runModel(model, xtrain2d, ytrain, xval2d)
            dfResiduals = pd.DataFrame(np.subtract(test_predictions, yval))
            dfResiduals.columns = ['farmfinal']

            # June:
            test_predictions = runModel(model, XtrainJune, ytrain, XtestJune)
            dfResiduals['farm43'] = np.subtract(test_predictions, yval)

            # July:
            test_predictions = runModel(model, XtrainJuly, ytrain, XtestJuly)
            dfResiduals['farm73'] = np.subtract(test_predictions, yval)

            # August:
            test_predictions = runModel(model, XtrainAugust, ytrain, XtestAugust)
            dfResiduals['farm104'] = np.subtract(test_predictions, yval)

            df.append(dfResiduals)

        if not args.testfile:
            basepath = args.inputfile.split('/')[:-2]
            out_dir_results = os.path.join(os.path.sep, *basepath, 'predictions', timeString + '-iterative')
            Path(out_dir_results).mkdir(parents=True, exist_ok=True)
        else:
            basepath = args.testfile.split('/')[:-2]
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

    parser.add_argument('--debug',
                        help='Verbose output for debugging.',
                        action='store_true')

    args = parser.parse_args()
    main(args)

