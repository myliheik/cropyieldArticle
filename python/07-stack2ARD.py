"""

23.10.2020 
27.1.2021 updated: replace DOY to order number 1,2,3,4,... during the season. We will need 4 datasets (June, July, August)
19.8.2021 added options 1) to merge given years and data sets, 2) outputdir
1.9.2021 saves in compressed numpy file instead of pickle
15.11.2021 oli törkeä virhe combineAllYears-funktiossa, pitää ajaa uudelleen kaikki
25.11.2021 oli törkeä virhe reshapeAndSave-funktiossa (pivot, reindex, reshape), pitää ajaa uudelleen kaikki

Combine annual stack-files into one array stack.

combineAllYears() reads all annuals into one big dataframe.

reshapeAndSave() pivots the dataframe by farmID and doy, converts to numpy array, fills with na (-> not ragged) and reshapes into 3D. Saves array and farmIDs into separate files.

RUN: 

python 07-stack2ARD.py -i /Users/myliheik/Documents/myCROPYIELD/dataStack_annual -o /Users/myliheik/Documents/myCROPYIELD/dataStack/ -f 1400 -y 2018 2019

# with 'time series by rank':
python 07-stack2ARD.py -i /Users/myliheik/Documents/myCROPYIELD/scratch/project_2001253/cropyieldII/cloudy/dataStack_annual -o /Users/myliheik/Documents/myCROPYIELD/scratch/project_2001253/cropyieldII/cloudy/dataStackRank -f 1120 -y 2020 -r

After this into 08-mergeTarget.py and 09-runNN.py.

In Puhti: module load geopandas (Python 3.8.) and also: pip install 'pandas==1.1.2' --user

"""
import glob
import os
import pandas as pd
import numpy as np
import pickle

from pathlib import Path

import argparse
import textwrap
from datetime import datetime


###### FUNCTIONS:

def load_intensities(filename):
    with open(filename, "rb") as f:
        data = pickle.load(f)
    return data

def save_intensities(filename, arrayvalues):
    with open(filename, 'wb+') as outputfile:
        pickle.dump(arrayvalues, outputfile)

def combineAllYears(data_folder3, setti, years):
    # read files in inputdir:
    s = pd.Series(glob.glob(data_folder3 + '/*.pkl'))

    filepaths = [] 

    for filename in s:
        for keyword1 in years:
            if keyword1 in filename:
                for keyword2 in setti:
                    if keyword2 in filename:
                        #print(filename)
                        filepaths.append(filename)
    #print(filepaths)                    
    # open all chosen years into one dataframe:
    allyears = pd.concat(map(pd.read_pickle, filepaths), sort=False)
    return allyears  

def reshapeAndSave(full_array_stack, out_dir_path, outputfile, rank):    
    # reshape and save data to 3D:
    print(f"\nLength of the data stack dataframe: {len(full_array_stack)}")

    if rank:
        dateVar = 'doyid'
    else:
        dateVar = 'doy'

    full_array_stack['doyid'] = full_array_stack.groupby(['farmID', 'band'])['doy'].rank(method="first", ascending=True).astype('int')

    #print(full_array_stack.sort_values(['farmID', 'doy']).tail(15))
    
    # printtaa esimerkkitila:
    #tmp = full_array_stack[full_array_stack['farmID'] == '2019_12026885_35VMH'][['farmID', 'doy', 'band', 'doyid']]
    #print(tmp.sort_values(['doy', 'band']))
    
    # printtaa sellaiset, joilla bin1 on 1:
    #print(len(full_array_stack[full_array_stack['bin1'] == 1]))
    # printtaa sellaiset, joilla bin32 on 1:
    #print(len(full_array_stack[full_array_stack['bin32'] == 1]))
    
    # printtaa sellaiset, joiden rivisumma ei ole 1:
    #print(full_array_stack[full_array_stack.drop(['farmID', 'doy', 'band', 'doyid'], axis = 1).sum(axis = 1) != 1])
    
    # printtaa näiden rivisummat:
    #tmp = full_array_stack[full_array_stack.drop(['farmID', 'doy', 'band', 'doyid'], axis = 1).sum(axis = 1) < 1]
    #print(len(tmp)) # jotain pyöristysvirhettä ehkäpä vain
    #print(tmp.drop(['farmID', 'doy', 'band', 'doyid'], axis = 1).sum(axis = 1))
    
    # Predictions to compare with forecasts: 15.6. eli DOY 166, that is pythonic 165.
    # and 15.7. eli DOY 196 
    # and 15.8. eli DOY 227
    # and the last DOY 243 -> the final state
    
    #june = full_array_stack[full_array_stack['doy'] <= 165]
    #print(june.sort_values(['doy', 'band']).tail(20))
    #print(june['doyid'].value_counts())

    #july = full_array_stack[full_array_stack['doy'] <= 195]
    #august = full_array_stack[full_array_stack['doy'] <= 226]

    
    final = full_array_stack
    #print(final['doyid'].value_counts())
    
    # Kuinka monta havaintoa per tila koko kesältä, mediaani?
    print("How many observations per farm in one season (median)?: ", float(final[['farmID', dateVar]].drop_duplicates().groupby(['farmID']).count().median()))
    # Kuinka monta havaintoa per tila koko kesältä, max?
    print("How many observations per farm in one season (max)?: ", float(final[['farmID', dateVar]].drop_duplicates().groupby(['farmID']).count().max()))
    # Kuinka monta havaintoa per tila koko kesältä, min?
    print("How many observations per farm in one season (min)?: ", float(final[['farmID', dateVar]].drop_duplicates().groupby(['farmID']).count().min()))

    # koko kausi:
    farms = final.farmID.nunique()
    doys = final[dateVar].nunique()
    bands = 10
    bins = 32
    pivoted = final.pivot(index=['farmID', dateVar], columns='band', values=[*final.columns[final.columns.str.startswith('bin')]])
    m = pd.MultiIndex.from_product([pivoted.index.get_level_values(0).unique(), pivoted.index.get_level_values(1).sort_values().unique()], names=pivoted.index.names)
    pt = pivoted.reindex(m, fill_value = 0)
    finalfinal = pt.to_numpy().reshape(farms, doys, bins, bands).swapaxes(2,3).reshape(farms,doys,bands*bins)
    
    outputfile2 = 'array_' + outputfile
    fp = os.path.join(out_dir_path, outputfile2)
    
    print(f"Shape of the 3D stack dataframe: {finalfinal.shape}")
    print(f"Output into file: {fp}")
    np.savez_compressed(fp, finalfinal)
    #save_intensities(fp, finalfinal)
    
    # save farmIDs for later merging with target y:
    farmIDs = pt.index.get_level_values(0).unique().str.rsplit('_',1).str[0].values
    print(f"\n\nNumber of farms: {len(farmIDs)}")
    outputfile2 = 'farmID_' + outputfile + '.pkl'
    fp = os.path.join(out_dir_path, outputfile2)
    print(f"Output farmIDs in file: {fp}")
    save_intensities(fp, farmIDs)
    

    
def main(args):
    
    try:
        if not args.outdir or not args.setti:
            raise Exception('Missing output dir argument or dataset label (e.g. test1110). Try --help .')

        print(f'\n\nstack2ARD.py')
        print(f'\nInput files in {args.inputdir}')

        # directory for input, i.e. annual results:
        data_folder3 = args.inputdir
        
        # directory for outputs:
        out_dir_path = args.outdir
        Path(out_dir_path).mkdir(parents=True, exist_ok=True)
        
        # years:
        years = args.ylist
        setti = args.setti
        
        # outputfilename:
        #outputfile = '-'.join(setti) + '-' + '-'.join(years) + '.pkl'
        outputfile = '-'.join(setti) + '-' + '-'.join(years)
        

                
        print("\nPresuming preprocessing done earlier. If not done previously, please, run with histo2stack.py first!")

        print("\nCombining the years and data sets...")
        allyears = combineAllYears(data_folder3, setti, years)
        reshapeAndSave(allyears, out_dir_path, outputfile, args.rank)
        

    except Exception as e:
        print('\n\nUnable to read input or write out results. Check prerequisites and see exception output below.')
        parser.print_help()
        raise e

if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                     epilog=textwrap.dedent(__doc__))

    parser.add_argument('-i', '--inputdir',
                        type=str,
                        help='Name of the input directory (where annual histogram dataframes are).',
                        default='.')
    parser.add_argument('-o', '--outdir',
                        type=str,
                        help='Name of the output directory.',
                        default='.')
    # is not true: cannot combine multiple data sets (crops), because farmID does not hold crop information -> duplicated farmIDs  
    parser.add_argument('-f', '--setti', action='store', dest='setti',
                         type=str, nargs='*', default=['1400'],
                         help='Name of the data set. Can be also multiple. E.g. -f 1310 1320.')
    #parser.add_argument('-f', '--setti', 
    #                    type=str,
    #                    default=['1400'],
    #                    help='Name of the data set. E.g. -f 1310.')
    parser.add_argument('-y', '--years', action='store', dest='ylist',
                       type=str, nargs='*', default=['2018', '2019', '2020', '2021'],
                       help="Optionally e.g. -y 2018 2019, default all")
    
    parser.add_argument('-r', '--rank',
                        help='If saving time series by rank of days.',
                        default=False,
                        action='store_true')
        
    args = parser.parse_args()
    main(args)



