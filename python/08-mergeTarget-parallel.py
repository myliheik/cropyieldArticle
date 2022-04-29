"""
20.8.2021 MY 
1.9.2021 MY reformatted output filename
14.2.2022 if NA in y, filter out from array, farmID and y.
15.2.2022 skip remove duplicates when training/testing models.
16.2. make parallel

Merge farmID with target y. 

Works also if multiple data sets are included in farmID-file.

RUN:

python 08-mergeTarget-parallel.py -i /Users/myliheik/Documents/myCROPYIELD/scratch/project_2001253/cropyieldIII/cloudy/dataStack/ \
-k /Users/myliheik/Documents/myCROPYIELD/referenceCROPYIELD2021/references-all.csv

python 08-mergeTarget-parallel.py -i /Users/myliheik/Documents/myCROPYIELD/scratch/project_2001253/cropyieldKunta/cloudy/dataStack_merged/ \
-k /Users/myliheik/Documents/myCROPYIELD/referenceCROPYIELD2021/references-kunnat.csv

python 08-mergeTarget-parallel.py -i /Users/myliheik/Documents/myCROPYIELD/scratch/project_2001253/cropyieldKunta/cloudless/dataStack_merged/ \
-k /Users/myliheik/Documents/myCROPYIELD/referenceCROPYIELD2021/references-kunnat.csv

"""
import pandas as pd
import numpy as np
import pickle
import os.path
from pathlib import Path
import argparse
import textwrap
import re
import glob
import utils
from itertools import repeat
from multiprocessing import Pool

maxcores = 18

# FUNCTIONS:
        
def makeTarget(inputfile, refefile, out_dir_path):
    # read array:
    arrayfile = utils.load_npintensities(inputfile)
    # read farmIDs:
    farmid = utils.readTargetID(inputfile)
    setti = utils.parse_xpath(inputfile)
    print(setti)
    fp1 = os.path.join(out_dir_path, 'y_' + setti + '.pkl')
    fp2 = os.path.join(out_dir_path, 'farmID_' + setti + '.pkl')
    fp3 = os.path.join(out_dir_path, inputfile.split('/')[-1])
    
    idsdf = pd.DataFrame(farmid)
    idsdf.columns = ['farmID']    
    #print(idsdf.tail())
    # read crop yields (target):
    targets = pd.read_csv(refefile)
    # merge:
    df = idsdf.merge(targets, how = 'left')
    if len(idsdf) == len(df):
        print(f'Length of farmIDs before and after merge match ({len(df)}).')
    if df['y'].isna().any():
        print(f'There are NAs!')
        # this means, some y not found. Let's filter also array and farmID.
        print(f"There are {df['y'].isna().sum()} NAs.")
        #print(arrayfile.shape, farmid.shape, len(targets))
        rowmaskNAs = np.array(df['y'].isna())

        arrayfileClear = arrayfile[~rowmaskNAs, :, :]
        farmidClear = df['farmID'][~rowmaskNAs]
        yClear = df['y'][~rowmaskNAs]
              
        print(f'Saving filtered data.')

        print(f'Saving target y to {fp1}.')
        utils.save_intensities(fp1, yClear)
        
        print(f'Saving farmID to {fp2}.')
        utils.save_intensities(fp2, farmidClear)
        
        print(f'Saving arrayfiles into {fp3}.')
        np.savez_compressed(fp3, arrayfileClear)    
        
        print(len(yClear), len(farmidClear), arrayfileClear.shape)
        
    else:
        print(f'Saving without the need to filter out NA data.')
        # Saving:

        print(f'Saving target y to {fp1}.')
        utils.save_intensities(fp1, df['y'])
        
        print(f'Saving farmID to {fp2}.')
        utils.save_intensities(fp2, df['farmID'])
        
        print(f'Saving arrayfiles into {fp3}.')
        np.savez_compressed(fp3, arrayfile)
        
# HERE STARTS MAIN:

def main(args):
    try:
        if not args.inputpath or not args.refefile:
            raise Exception('Missing farmID or target filepath argument. Try --help .')

        print(f'\n08-mergeTarget-parallel.py')
        print(f'\nStacked data in: {args.inputpath}')

        # directory for results:
        if 'median' in args.inputpath:
            out_dir_path = os.path.join(str(Path(args.inputpath).parents[0]),  'medianStack_ard')
            Path(out_dir_path).mkdir(parents=True, exist_ok=True)
        else:
            out_dir_path = os.path.join(str(Path(args.inputpath).parents[0]),  'dataStack_ard')
            Path(out_dir_path).mkdir(parents=True, exist_ok=True)
            
        
        print("\nMerging farmID and crop yields to make target set y...")
        filenames = glob.glob(args.inputpath + 'array*.npz')
        #print(filenames)
        if filenames:
            p = Pool(maxcores)
            p.starmap(makeTarget, zip(filenames, repeat(args.refefile), repeat(out_dir_path)))
            # wait for all tasks to finish
            p.close()


            #for fp in filenames:
            #    print(fp)
            #    makeTarget(fp, args.refefile, out_dir_path)
                
                
            
        print(f'\nDone.')

    except Exception as e:
        print('\n\nUnable to read input or write out statistics. Check prerequisites and see exception output below.')
        parser.print_help()
        raise e
        
        
if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                     epilog=textwrap.dedent(__doc__))

    parser.add_argument('-i', '--inputpath',
                        help='Path to data directory (dataStack_duplicatesRemoved).',
                        type=str)

    parser.add_argument('-k', '--refefile',
                        help='Filename of crop yields data. Remove bad data beforehand (like suspiciously low yields).',
                        type=str)
    parser.add_argument('--debug',
                        help='Verbose output for debugging.',
                        action='store_true')

    args = parser.parse_args()
    main(args)
