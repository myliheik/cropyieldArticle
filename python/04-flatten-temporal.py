"""
MY 2022-03-19

Flatten observations' temporal dimension: 11-days combined

RUN: python 04-flatten-temporal.py -i cloudless/results_1110_2018 -o cloudless/results_1110_2018 \
-c 19

WHERE:
i: input dir
o: output dir
c: Number of cores to use

"""

import os

import argparse
import textwrap
from pathlib import Path

from itertools import repeat
from multiprocessing import Pool
        
        
def concatSeparateDatesIntoOne(arrayfile, datadir, out_dir_path):

    if arrayfile.endswith('.csv') and arrayfile.startswith('array_'):
        #print(arrayfile)
        tile = arrayfile.split('_')[1]
        date0 = arrayfile.split('_')[2][:-3]
        date = int(arrayfile.split('_')[2][-2:])
        month = arrayfile.split('_')[2][-3:-2] # works only for Jan-Sept
        monthNext = str(int(month) + 1)
        tail = arrayfile.split('_')[3]

        if date < 11:
            newdate = month + '11'
        elif (date >= 11 and date < 21):
            newdate = month + '21'
        else:
            newdate = monthNext + '01'
            
        newarrayfile = 'array_' + tile + '_' + date0 + newdate + '_' + tail
        #print(newarrayfile)
                
        arraypath = os.path.join(datadir,arrayfile)
        outputpath = os.path.join(out_dir_path,newarrayfile)

        os.system('cat {} >> {}' .format(str(arraypath), str(outputpath)))
        
        # Done.
        
        
def main(args):
    try:
        if not args.inputpath or not args.outdir:
            raise Exception('Missing input or output dir argument. Try --help .')

        print(f'\n\n04-flatten-temporal.py')
        print(f'\nInput files in {args.inputpath}')

        fp = args.inputpath
            
        print(f'\nSaving time flattened arrays into {args.outdir}...')
                
        datadir = args.inputpath
        
        list_of_files = os.listdir(datadir)
        p = Pool(args.ncores)
        p.starmap(concatSeparateDatesIntoOne, zip(list_of_files, repeat(datadir), repeat(args.outdir)))
        # wait for all tasks to finish
        p.close()

                    
    except Exception as e:
        print('\n\nUnable to read input or write out results. Check prerequisites and see exception output below.')
        parser.print_help()
        
        raise e

if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                     epilog=textwrap.dedent(__doc__))
    parser.add_argument('-i', '--inputpath',
                        type=str,
                        help='Path to the directory with reflectance values.')
    parser.add_argument('-o', '--outdir',
                        type=str,
                        help='Name of the output directory.')
    parser.add_argument('-c', '--ncores',
                        type=int,
                        help='Number of cores to use.',
                        default = 1)
        
    
    args = parser.parse_args()
    main(args)
