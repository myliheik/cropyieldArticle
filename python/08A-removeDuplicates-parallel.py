"""
MY
13.2.2022 Remove duplicates, compute marix addition.

Reads all files in input path. Handles duplicates and corrects farmID file. Saves into *_duplicatesRemoved

Do this before 08-mergeTarget.py.

RUN:

python 08A-removeDuplicates.py -i /Users/myliheik/Documents/myCROPYIELD/scratch/project_2001253/cropyieldKunta/cloudy/dataStack/

python 08A-removeDuplicates.py -i /Users/myliheik/Documents/myCROPYIELD/scratch/project_2001253/cropyieldIII/cloudy/dataStack/

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

from multiprocessing import Pool

maxcores = 18

# FUNCTIONS:

def theworks(fp, inputpath, out_dir_path):
            
    arrayfile = utils.load_npintensities(fp)
    farmid = utils.readTargetID(fp)
    #print(list(duplicates(farmid)))
    rowmaskDuplicated = np.array([True if x in list(duplicates(farmid)) else False for x in farmid])


    # if there are duplicates:
    if any(rowmaskDuplicated):
        # save the unique cases first:
        arrayfileClear = arrayfile[~rowmaskDuplicated, :, :]
        farmidClear = farmid[~rowmaskDuplicated]

        for farm in list(unique_everseen(duplicates(farmid))):
            alist = arrayfile[farm == farmid, :, :]
            # matrix addition of multiple arrays:
            uusi = np.add.reduce(alist) 

            arrayfileClear = np.concatenate([arrayfileClear, uusi[np.newaxis,:,:]])
            farmidClear = np.append(farmidClear, farm)


    else: # if there are no duplicates at all
        arrayfileClear = arrayfile
        farmidClear = farmid
        print('There are no duplicates at all.')

    # last check:
    if arrayfileClear.shape[0] != farmidClear.shape[0]:
        print(f'List lengths not matching! Check {fp}')

    print(f'There was {sum([True if x in list(duplicates(farmid)) else False for x in farmid])} duplicates.')
    print(f'Old array shape: {arrayfile.shape}')
    print(f'Old farm list shape: {farmid.shape}')
    print(f'New array shape: {arrayfileClear.shape}')
    print(f'New farm list shape: {farmidClear.shape}')

    # Saving:
    tail = utils.parse_xpath(fp)
    print(tail)
    fp2 = 'farmID_' + tail + '.pkl'
    print(f'Saving farmID files into {os.path.join(out_dir_path, fp2)}.')
    utils.save_intensities(os.path.join(out_dir_path, fp2), farmidClear)
    fp3 = fp.split('/')[-1]
    print(f'Saving arrayfiles into {os.path.join(out_dir_path, fp3)}.')
    np.savez_compressed(os.path.join(out_dir_path, fp3), arrayfileClear)

        

# HERE STARTS MAIN:

def main(args):  
    try:
        if not args.inputpath:
            raise Exception('Missing input dir argument. Try --help .')

        print(f'\n\n08A-removeDuplicates-parallel.py')
        print(f'\nInput files in {args.inputpath}')

        datadir = args.inputpath

        # directory for results:
        out_dir_path = os.path.dirname(datadir) + "_duplicatesRemoved"
        Path(out_dir_path).mkdir(parents=True, exist_ok=True)
        
        list_of_files = glob.glob(inputpath + 'array*.npz')
        if list_of_files:
            p = Pool(maxcores)
            p.starmap(theworks, zip(list_of_files, repeat(datadir), repeat(out_dir_path)))
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

    args = parser.parse_args()
    main(args)


