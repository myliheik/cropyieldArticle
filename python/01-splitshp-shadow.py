"""

2020-06-01 MY

Usage:
python splitshp-shadow.py --s2tiles suomiTiles.shp \
--fullshapefile shapefile --outshpdir satotutkimus-shpPerTile/ --out_file farmIDtile.tsv

Modified version of EODIE splitshp.py

"""

import os
from osgeo import osr
import subprocess
import sys

import pandas as pd
import geopandas as gpd

import argparse
import textwrap
import pathlib


def main(args):
    try:
        if not args.fullshapefile or not args.s2tiles:
            raise Exception('Missing shapefile argument. Try --help .')

        print(f'\n\nsplitshp-shadow.py')
        print(f'\nSentinel2 tiles: {args.s2tiles}')
        print(f'ESRI shapefile parcels: {args.fullshapefile}')
        out_dir_path = pathlib.Path(os.path.expanduser(args.outshpdir))
        out_dir_path.mkdir(parents=True, exist_ok=True)


        out_file = args.out_file

        print('Reading parcels...')

        def checkProjection(myshp):
            print('INFO: checking the projection of the inputfile now')
            head, tail = os.path.split(myshp)
            root, ext = os.path.splitext(tail)
            rootprj = root + '.prj'
            projectionfile = os.path.join(head, rootprj)
            prj_file = open(projectionfile , 'r')
            prj_text = prj_file.read()
            srs = osr.SpatialReference()
            srs.ImportFromESRI([prj_text])
            srs.AutoIdentifyEPSG()
            epsgcode = srs.GetAuthorityCode(None)
            if epsgcode == '3067':
                print('INFO: input shapefile has EPSG 3067, that works!')
                return myshp
            else:
                reprojectedshape = os.path.join(head, root + '_reprojected_3067'+ ext)
                #if not os.path.exists(reprojectedshape):
                reprojectcommand = 'ogr2ogr -t_srs EPSG:3067 ' + reprojectedshape + ' ' + myshp
                subprocess.call(reprojectcommand, shell=True)
                print('INFO: input shapefile had other than EPSG 3067, but was reprojected and works now')
                return reprojectedshape


        # bringing all input shapefiles to EPSG 3067 
        print('Parcel shapefile: ')
        fullshapefile2 = checkProjection(args.fullshapefile)
        print('Sentinel2 shapefile: ')
        s2tiles2 = checkProjection(args.s2tiles)


        # filename:
        originalname = os.path.splitext(os.path.split(fullshapefile2)[-1])[0]


        # Tehdään loput geopandalla:

        tiles = gpd.read_file(s2tiles2)
        parcelshp = gpd.read_file(fullshapefile2)

        print(f'There are ', len(parcelshp), ' parcels in the input shapefile.')
        
        # for bookkeeping
        df = pd.DataFrame(columns = ['farmID', 'Tile'])

        for index, row in tiles.iterrows(): # Looping over all tiles
            tilename = row['Name']

            # is there any parcels on this tile's BBOX:
            xmin, ymin, xmax, ymax = row['geometry'].bounds
            parcels = parcelshp.cx[xmin:xmax, ymin:ymax]

            if not parcels.empty:

                res_intersection = parcels['geometry'].within(row['geometry'])
                if any(res_intersection):
                    parcelsToFile = parcels[res_intersection]
                    outshpname = os.path.join(args.outshpdir,originalname + '_' + str(tilename)+'.shp')        
                    parcelsToFile.crs = '+proj=utm +zone=35 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs'
                    parcelsToFile.to_file(outshpname)

                    writeParcels = pd.DataFrame(parcelsToFile['farmID'])
                    writeParcels['Tile'] =  tilename
                    #print(writeParcels)
                    writeParcels.to_csv(out_file, mode='a', header=False)

                    df = df.append(writeParcels, ignore_index = True) 

        print(f'Intersecting farmIDs and tiles saved to {out_file}.')
        #df.to_csv(out_file, sep = '\t', index = False)

        print(f'\nDone.')

    except Exception as e:
        print('\n\nUnable to read input or write out files. Check prerequisites and see exception output below.')
        parser.print_help()
        raise e


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                     epilog=textwrap.dedent(__doc__))
    parser.add_argument('-s', '--s2tiles',
                        type=str,
                        help='Sentinel-2 tiles.')
    parser.add_argument('-a', '--fullshapefile',
                        type=str,
                        help='ESRI shapefile containing a set of polygons (.shp with its auxiliary files)')
    parser.add_argument('-d', '--outshpdir',
                        help='Directory for output shp files',
                        type=str,
                        default='.')
    parser.add_argument('-o', '--out_file',
                    help='Output (e.g. .tsv) tab-separated file containing farmID and the tile it was found at.',
                    type=str,
                    default='farmIDtile.tsv')

    args = parser.parse_args()
    main(args)
    
