############################
"""
16.6.2020 modified by MY
27.8.2021 remove normalization of histograms, do normalization later.
24.9.2021  changed to_csv to save in UNIX forma (not DOS)
20.11.2021 removed if below (or over) bin range, set 1st (or last) value 1, others 0. 

From Samatha Wittke's code:
preparing for ML:
input array csvs from main program, each line representing one field, first number being ID of the field
output similar csv with each line representing one field, first number being ID of field, followed by x = bins numbers representing histogram values


RUN: python histogramize-shadow.py -i input -o output -b B8A -n nrbins -l 2 -h 2000
"""
######################


import os
import numpy as np
import sys
import csv
import os

import argparse
import textwrap
import pathlib

            
def to_csv(csvfile, myarray):

    csvfile = csvfile.replace('array_','histogram_')
    #with open(csvfile, "w") as f:
    with open(csvfile, "w", newline='') as f:
        writer = csv.writer(f, lineterminator=os.linesep)
        writer.writerows(myarray)

def make_histogram(inarray,bin_seq):

    histo1, _ = np.histogram(inarray, bin_seq, density=False)
    return histo1

def main(args):
    try:
        if not args.inputpath or not args.band:
            raise Exception('Missing input or output dir argument or band number (e.g. B8A). Try --help .')

        print(f'\n\nhistogramize-shadow.py')
        print(f'\nInput files in {args.inputpath}')
        print(f'Band: {args.band}')
        out_dir_path = pathlib.Path(os.path.expanduser(args.outdir))
        out_dir_path.mkdir(parents=True, exist_ok=True)

        datadir = args.inputpath
        band = args.band
  
        bin_seq = np.linspace(args.minimum,args.maximum,args.nrbins+1)
        
        #print('Reading arrayfiles...')

        for arrayfile in os.listdir(datadir):
            if arrayfile.endswith(band + '.csv') and arrayfile.startswith('array_'):
                #print(arrayfile)
                histlist = []
                arraypath = os.path.join(datadir,arrayfile)
                outputpath = os.path.join(out_dir_path,arrayfile)
                with open(arraypath, "r") as f:
                    reader = csv.reader(f)
                    for line in reader:
                        myid = [line[0]]
                        #if myid:
                        line = [int(elem) for elem in line if not '_' in elem] 
                        #print(line)
                        #if min(line) >= args.maximum:
                        #    #hist = [float(0)]*(args.nrbins-1); hist.append(float(1)) 
                        #    hist = [0]*(args.nrbins-1); hist.append(1) 
                        #elif max(line) <= args.minimum:
                        #    hist = [1]; hist.extend([0]*(args.nrbins-1))
                        #else:
                        #    hist = make_histogram(line, bin_seq)
                        hist = make_histogram(line, bin_seq)
                        #print(hist)
                        myid.extend(hist)
                        hist2 = myid
                        #print(hist2)
                        histlist.append(hist2)
                        #print(histlist)

                    to_csv(outputpath,histlist)
                    
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
    parser.add_argument('-n', '--nrbins',
                        type=int,
                        default=16,
                        help='Number of bins.')
    parser.add_argument('-b', '--band',
                        help='Band number (e.g. B02)',
                        type=str)
    parser.add_argument('-l', '--minimum',
                        help='The lower range of the bins.',
                        type=int)
    parser.add_argument('-u', '--maximum',
                        help='The upper range of the bins.',
                        type=int)
    parser.add_argument('-o', '--outdir',
                        type=str,
                        help='Name of the output directory.',
                        default='.')

    args = parser.parse_args()
    main(args)
