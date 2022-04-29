"""
17.8.2020 MY
23.10.2020 no more features as tuples but as array.
19.8.2021 modified to save into dataStack_annuals (instead of dataStack_temp).
31.8.2021 added option to use tempdir

Make histo-files into annual dataframes. Saves into outputdir_annuals.

createMissingFiles() checks if all 10 bands exists per observation (farm). If not, makes a copy of any band from the same doy and sets all values to zero.

getAttributesFromFilename() adds tile-, DOY ja band information from filename to data.

mergeAllGetNumpyArrays() makes one big dataframe for one year. Save to outputdir_annuals.

testing(outputfile) tests if output file is ok.

RUN: 

python 06-histo2stack.py -i histo_test1110_2016 -n 32 -o dataStack -f test1110_2016.pkl -t TEMPDIRPATH

After this into 07-stack2ARD.py.

"""

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

def createMissingFiles(datadir):
    # List all files
    list_of_files = os.listdir(datadir)

    # histogram_35VNL_20200830_B8A.csv
    # This removes the .csv and splits the name to three parts
    list_of_filename_parts = [i.replace(".csv","").split("_") for i in list_of_files]
    
    # Makes a df of all filenames
    df = pd.DataFrame(list_of_filename_parts, columns=['histo','tile','date','band'])
    #print(df.head())

    # Group and iterate by date, see if bands are missing
    grouped_df = df.groupby(['date', 'tile'])

    # Bands as text that should exist
    bands = ['B02','B03','B04','B05','B06','B07','B08','B8A','B11','B12']
    
    # Iterate
    for name, date_group in grouped_df:
        #print(name[1])
        existing_bands = list(date_group['band'])
        for band in bands:
            if band not in existing_bands:
              	# Band is missing create a mockup dataframe and save
                print(f"For date {name} band {band} is missing!")

                ### Copy from existing band, same date, set all values to 0 (or np.nan)
                
                temp_filename = os.path.join(datadir,"histogram_" + name[1] + "_" + name[0] + "_" + existing_bands[0] + ".csv")
                #print(temp_filename)
                dftemp = pd.read_csv(temp_filename, encoding='utf-8', header = None)
                #print(dftemp.iloc[:, 1:])
                dftemp.iloc[:,1:] = 0
                #print(dftemp)

                output_filename = os.path.join(datadir,"histogram_" + name[1] + "_" + name[0] + "_" + band + ".csv")
                print(f"Saving a new file named {output_filename}")
                dftemp.to_csv(output_filename,encoding='utf-8',index=False, header=False)

def getAttributesFromFilename(datadir, data_folder2):
    ### Add date and band to every file as columns

    # Loop files in data_folder
    for filename in os.listdir(datadir):
        if filename.endswith('.csv') and filename.startswith('histogram_'):
            #print(filename)
            try:
                df = pd.read_csv(os.path.join(datadir,filename), encoding='utf-8', header = None)
            except pd.errors.EmptyDataError:
                print(f'{os.path.join(datadir,filename)} was empty. Skipping.')
                continue
            # Add tile, band and date from filename to columns
            df['tile'] = filename.split("_")[1]
            pvm = filename.split("_")[2]
            df['doy'] = datetime.strptime(pvm, '%Y%m%d').timetuple().tm_yday
            #print(doy)
            df['band'] = filename.split("_")[3].replace(".csv","")
            #print(band)

            ### Write to data_folder2
            df.to_csv(os.path.join(data_folder2,filename), encoding='utf-8',index=False, header=False)
            
def mergeAllGetNumpyArrays(data_folder2, data_folder3, bins, outputfile):
    ### Merge all files to one big dataframe

    df_array = []

    ### Read files to pandas, add the dataframes to the array
    for filename in os.listdir(data_folder2):
        df = pd.read_csv(os.path.join(data_folder2,filename), encoding="utf-8", header=None)
        df.rename(columns={(bins + 1): 'tile', (bins + 2): 'doy', (bins + 3): 'band'}, inplace=True)
        try:
            df['farmID'] = df[0] + '_' + df['tile']
        except Exception as e:
            print(f'\n\nThere is something wrong with file {os.path.join(data_folder2,filename)}...')
            print('Check that you have set the right number of bins!')
            raise e
        old_names = df.columns.tolist()[1:bins+1]
        new_names = []
        for bin in range(bins):
            new_names.append("bin" + str(bin+1))

        df = df.rename(columns=dict(zip(old_names, new_names)))
        df = df.drop(0, axis = 1)
        df = df[['farmID', 'band','doy', *df.columns[df.columns.str.startswith("bin")]]]
        df_array.append(df)

    ### Make a big dataframe out of the list of dataframes
    all_files_df = pd.concat(df_array)
    ### And save to temp:
    save_intensities(os.path.join(data_folder3,outputfile), all_files_df)
    
    return all_files_df

def addDOYrank(all_files_df, out_dir_path, outputfile):
    #print(all_files_df.head())
    days = all_files_df.doy.sort_values().unique()
    days_dict = dict(zip(days, range(len(days))))
    print(days_dict)
    all_files_df2 = all_files_df
    return all_files_df2
    
def testing(all_files_df, out_dir_path, outputfile):
    print("Output written to file: ", outputfile)

    tmp2 = all_files_df.groupby(['doy', 'farmID']).count()#.unstack().fillna(0)

    if tmp2[tmp2.band != 10]['band'].any():
        print("Some bands missing!")
    else:
        print("All farms have full 10 bands!")

    # kuinka monta tilaa mukana?
    print("How many farms are observed from one or several S2 granules?:", len(all_files_df[['farmID']].drop_duplicates()))
    
    # kuinka monta tilaa mukana oikeasti?
    farmIDs = all_files_df['farmID'].str.rsplit('_',1).str[0]
    print("How many farms we really have?: ", len(farmIDs.drop_duplicates()))
        
    # Kuinka monta havaintoa per tila koko kesältä, mediaani?
    print("How many observations per farm in one season (median)?: ", float(all_files_df[['farmID', 'doy']].drop_duplicates().groupby(['farmID']).count().median()))

    # kuinka monta havaintoa per päivä, mediaani?
    print("How many observations per day (median)?: ", float(all_files_df[['farmID', 'doy']].drop_duplicates().groupby(['doy']).count().median()))

              
def main(args):
    
    try:
        if not args.inputpath or not args.outdir:
            raise Exception('Missing input or output dir argument or bin number (e.g. 32). Try --help .')

        print(f'\n\nhisto2stack.py')
        print(f'\nInput files in {args.inputpath}')
        print(f'Bins: {args.bins}')
        out_dir_path = Path(os.path.expanduser(args.outdir))
        out_dir_path.mkdir(parents=True, exist_ok=True)

        datadir = args.inputpath
        bins = args.bins
        outputfile = args.outfile

        # temp directory for annual histograms:
        data_folder2 = args.tmpdir
        Path(data_folder2).mkdir(parents=True, exist_ok=True)

        # directory for annual dataframes:
        data_folder3 = args.outdir + "_annual"
        Path(data_folder3).mkdir(parents=True, exist_ok=True)


        createMissingFiles(datadir)
        getAttributesFromFilename(datadir, data_folder2)
        
        # tämä tekee jo varsinaisen osuuden:
        all_files_df = mergeAllGetNumpyArrays(data_folder2, data_folder3, bins, outputfile) 
        
        # loput on testausta:
        all_files_df = load_intensities(os.path.join(data_folder3,outputfile))
        all_files_df = addDOYrank(all_files_df, out_dir_path, outputfile)
        testing(all_files_df, out_dir_path, outputfile)


    except Exception as e:
        print('\n\nUnable to read input or write out results. Check prerequisites and see exception output below.')
        parser.print_help()
        raise e

if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                     epilog=textwrap.dedent(__doc__))
    parser.add_argument('-i', '--inputpath',
                        type=str,
                        help='Path to the directory with histogram csv files.',
                        default='.')
    parser.add_argument('-n', '--bins',
                        type=int,
                        default=16,
                        help='Number of bins.')
    parser.add_argument('-o', '--outdir',
                        type=str,
                        help='Name of the output directory.',
                        default='.')
    parser.add_argument('-f', '--outfile',
                        type=str,
                        help='Name of the output file.',
                        default='.')
    parser.add_argument('-t', '--tmpdir',
                        type=str,
                        help='Name of the temp directory.',
                        default='.')
    args = parser.parse_args()
    main(args)


