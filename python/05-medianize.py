"""
2021-12-01

python 05-medianize.py -i input -o output

"""


import os
import numpy as np
import sys
import csv
import os

import argparse
import textwrap
import pathlib

            
def to_csv(csvfile, myarray):

    csvfile = csvfile.replace('array_','median_')
    #with open(csvfile, "w") as f:
    with open(csvfile, "w", newline='') as f:
        writer = csv.writer(f, lineterminator=os.linesep)
        writer.writerows(myarray)

def main(args):
    try:
        if not args.inputpath:
            raise Exception('Missing input or output dir argument. Try --help .')

        print(f'\n\n05-medianize.py')
        print(f'\nInput files in {args.inputpath}')

        out_dir_path = pathlib.Path(os.path.expanduser(args.outdir))
        out_dir_path.mkdir(parents=True, exist_ok=True)

        datadir = args.inputpath

          
        #print('Reading arrayfiles...')

        for arrayfile in os.listdir(datadir):
            if arrayfile.startswith('array_'):
                #print(arrayfile)
                lista = []
                arraypath = os.path.join(datadir,arrayfile)
                outputpath = os.path.join(out_dir_path,arrayfile)
                with open(arraypath, "r") as f:
                    reader = csv.reader(f)
                    
                    for line in reader:
                        myid = [line[0]]
                        #if myid:
                        line = [int(elem) for elem in line if not '_' in elem] 
                        median = np.median(line)
                        myid.extend([int(median)])
                        median2 = myid
                        lista.append(median2)
                        
                    if lista:
                        to_csv(outputpath,lista)
                    
    except Exception as e:
        print('\n\nUnable to read input or write out results. Check prerequisites and see exception output below.')
        parser.print_help()
        raise e

if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                     epilog=textwrap.dedent(__doc__))
    parser.add_argument('-i', '--inputpath',
                        type=str,
                        help='Path to the directory with array csv files.',
                        default='.')

    parser.add_argument('-o', '--outdir',
                        type=str,
                        help='Name of the output directory.',
                        default='.')

    args = parser.parse_args()
    main(args)


