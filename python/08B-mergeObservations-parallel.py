"""
MY
9.3.2022 Merge selected observations (e.g. by region), compute marix addition.

Reads all files in input path. Handles duplicates and corrects farmID file. Saves into *_merged

Run only for single years (regions are not meant to be used for training).

After this run 08-mergeTarget.py.

RUN:

python 08B-mergeObservations-parallel.py -i /Users/myliheik/Documents/myCROPYIELD/scratch/project_2001253/cropyieldArticle/cloudy/dataStack/ -o /Users/myliheik/Documents/myCROPYIELD/scratch/project_2001253/cropyieldKunta/cloudy/dataStack/ \
-k /Users/myliheik/Documents/myCROPYIELD/satotilatkunnittain/satotilalistaJaKunta.csv -c 8

python 08B-mergeObservations-parallel.py -i /Users/myliheik/Documents/myCROPYIELD/scratch/project_2001253/cropyieldArticle/cloudless/dataStack/ -o /Users/myliheik/Documents/myCROPYIELD/scratch/project_2001253/cropyieldKunta/cloudless/dataStack/
-k /Users/myliheik/Documents/myCROPYIELD/satotilatkunnittain/satotilalistaJaKunta.csv -c 8

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
from iteration_utilities import duplicates, unique_everseen
from itertools import repeat
from multiprocessing import Pool

maxcores = 18

# FUNCTIONS:

def theworks(fp, inputpath, out_dir_path, chosenFarms):
    
    tail = utils.parse_xpath(fp)
    print(f'Starting processing {tail}')
            
    arrayfile = utils.load_npintensities(fp)
    farmid = utils.readTargetID(fp)

    rowmask = np.array([True if x in list(chosenFarms['farmID'].tolist()) else False for x in farmid])
    

    # if there are any farms:
    if any(rowmask):
        
        newfarmid = farmid[rowmask]
        newarray = arrayfile[rowmask, :, :]

        newdf = pd.DataFrame(newfarmid, columns = ['farmID'])
        newdf2 = newdf.merge(chosenFarms)
        newdf2[['Year', 'farm_ID', 'Crop']] = newdf2['farmID'].str.split('_', expand = True)
        newfarmid = newdf2['Year'] + '_' +  newdf2['KUNTA_KNRO_VUOSI'].astype('str') + '_' + newdf2['Crop']

        # are there cases (regions) with only one observation (tila)?
        if set(newfarmid) - set(duplicates(newfarmid)):
            print(f'There are cases (regions) with only one observation (tila): {len(set(newfarmid) - set(duplicates(newfarmid)))}')
            print(f'Namely, these: {len(set(newfarmid) - set(duplicates(newfarmid)))}')
    
        l = []
        lfarmid = []

        print(f'There are {len(list(unique_everseen(duplicates(newfarmid))))} duplicated regions.')

        for farm in list(unique_everseen(duplicates(newfarmid))):
            print(farm)
            alist = newarray[[i in farm for i in newfarmid], :, :]
            # matrix addition of multiple arrays:
            uusi = np.add.reduce(alist) 
            #l.append(uusi[np.newaxis,:,:])
            l.append(uusi)
            lfarmid.append(farm)

        newarrayMerged = np.asarray(l)


        # last check:
        if newarray.shape[0] != newfarmid.shape[0]:
            print(f'List lengths not matching! Check {fp}')

        print(f'There were {rowmask.sum()} chosen farms.')
        print(f'Old array shape: {arrayfile.shape}')
        print(f'Old farm list shape: {farmid.shape}')
        print(f'New array shape: {newarrayMerged.shape}')
        print(f'New farm list shape: {len(lfarmid)}')

        # Saving:

        fp2 = 'farmID_' + tail + '.pkl'
        print(f'Saving farmID files into {os.path.join(out_dir_path, fp2)}.')
        utils.save_intensities(os.path.join(out_dir_path, fp2), lfarmid)
        fp3 = fp.split('/')[-1]
        print(f'Saving arrayfiles into {os.path.join(out_dir_path, fp3)}.')
        np.savez_compressed(os.path.join(out_dir_path, fp3), newarrayMerged)
        
    else: # if there are no duplicates at all
        print(f'There are no selected farms in {fp}.')

        

# HERE STARTS MAIN:

def main(args):  
    try:
        if not args.inputpath:
            raise Exception('Missing input dir argument. Try --help .')

        print(f'\n\n08B-mergeObservations-parallel.py')
        print(f'\nInput files in {args.inputpath}')

        datadir = args.inputpath

        # directory for results:
        out_dir_path = os.path.dirname(args.outputpath) + "_merged"
        Path(out_dir_path).mkdir(parents=True, exist_ok=True)
        
        
        chosenFarms = pd.read_csv(args.kunnat)
        
        # only annual data sets:
        list_of_files = glob.glob(datadir + 'array_1' + ('[0-9]' * 3) + '-20' + ('[0-9]' * 2) + '.npz')
        if list_of_files:
            p = Pool(maxcores)
            p.starmap(theworks, zip(list_of_files, repeat(datadir), repeat(out_dir_path), repeat(chosenFarms)))
            # wait for all tasks to finish
            p.close()
            
        #theworks(datadir, out_dir_path)
        
        print('Done.')

    except Exception as e:
        print('\n\nUnable to read input or write out results. Check prerequisites and see exception output below.')
        parser.print_help()
        raise e

if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                     epilog=textwrap.dedent(__doc__))
    parser.add_argument('-i', '--inputpath',
                        type=str,
                        help='Path to the directory with stacked array files.',
                        default='.')
    parser.add_argument('-o', '--outputpath',
                        type=str,
                        help='Path to the output directory with merged array files.',
                        default='.')
    parser.add_argument('-k', '--kunnat',
                        type=str,
                        help='Path to the file with kunnat, tilat.')
    parser.add_argument('-c', '--ncores',
                        type=int,
                        help='Number of cores to use.',
                        default = 1)

    args = parser.parse_args()
    main(args)


