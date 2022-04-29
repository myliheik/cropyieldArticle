"""
Originally Samantha Wittke in 2020 for EODIE.

Modified by Maria Yli-Heikkila, Markku Luotamo

2020-11-12 Commented out meta data writer (extractmeta), useful only for testing purposes.
2021-08 ML added cloud masking
2021-09-06 MY added option to use tempdir
2021-09-24 MY changed to save csv in UNIX format, not DOS; not saving empty files anymore

USAGE:
python 03-arrayextractor.py -f $name -shp $shppath -p $projectpath -jn ${ID} -id $idname -r 10 -t $TEMPDIRPATH

WHERE:
-f: raster file path
-shp: polygons shapefile path
-p: output path
-jn: job number ID
-id: name of the identifier variable in shapefile (e.g. 'parcelID')
-r: for multi-band operation you must specify a common target resolution (e.g. 10)
-t: temporary directory path


"""

import csv
import os
import re
from datetime import datetime
from glob import glob
from shutil import copyfile
from typing import Optional
from rasterstats import zonal_stats
import functools

import numpy as np
import rasterio
import shapeobject
import userinput
import traceback

BANDS = ['B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08', 'B8A', 'B11', 'B12']

FILTER_OUT_SEN2COR_CLASSES = [0, 1, 3, 8, 9, 10] # No data, Cloud Shadows + Clouds medium+high probability + Cirrus
#[0, 1, 3, 8, 9, 10]
CLOUD_MASK_RESO_M = 20
NO_DATA = np.nan
INVALID = 0

#direct array extraction

def main():

    ui = userinput.UserInput()
    jobnumber = ui.jobnumber
    bandpathtxt = ui.bandpath
    tile = parse_tile_from_path(bandpathtxt)
    shapedir = ui.shapedir
    namelist = os.listdir(shapedir)[0].split('_')
    shapename = '_'.join(namelist[:-1])
    tileshapename = shapename + '_' + tile
    shapefile = os.path.join(shapedir, tileshapename)
    projectname = ui.projectname
    if not os.path.exists(projectname):
       print(f'Creating output direcory {projectname}...')
       os.makedirs(projectname)
    if ui.tmpdir:
        tmpdir = ui.tmpdir
    else:
        tmpdir = projectname
    shpfile = None
    if not jobnumber is None:
        for ext in ['.shp','.shx','.prj','.dbf']:
            shp = shapefile + ext
            if os.path.isfile(shp):
                #print(shp)
                jobdir = os.path.join(tmpdir,'temp',jobnumber)
                dst = os.path.join(jobdir, tileshapename + ext)
                if not os.path.exists(jobdir):
                    os.makedirs(jobdir)
                copyfile(shp, dst)
                if dst.endswith('.shp'):
                    shpfile = dst
    else:
        shpfile = shapefile + '.shp'

    extractarray(bandpathtxt, shpfile, tile, projectname, ui)


def extractarray(raster_path,
                 shpfile,
                 tile,
                 projectname,
                 ui):

    cloud_mask_path: Optional[str]
    band_paths: [str]

    cloud_mask_path, band_paths = expand_raster_paths(raster_path, ui)

    shapeobj: shapeobject.ShapeObject = shapeobject.ShapeObject(shpfile)

    if cloud_mask_path:
        shpfile: str = shapeobj.checkProjection(cloud_mask_path)
    else:
        shpfile: str = shapeobj.checkProjection(raster_path)

    if cloud_mask_path:
        assert ui.target_resolution_m, 'For cloud masking you must specify a common target resolution.'
        try:
            parcel_cloud_masks: [dict] = crop_band_raster_per_parcel(cloud_mask_path, shpfile, ui.target_resolution_m,
                                                                     resampling=rasterio.enums.Resampling.nearest)
        except Exception as e:
            print(f'Error reading cloud mask for tile {tile} from "{cloud_mask_path}"')
            raise e

    for band_path in band_paths:

        date: str = parse_date_from_path(band_path)
        band: str = parse_band_from_path(band_path)

        if ui.target_resolution_m:
            target_resolution_m: int = ui.target_resolution_m
        else:
            target_resolution_m: int = parse_resolution_from_path(band_path)
        try:
            band_raster_per_parcel: [dict] = crop_band_raster_per_parcel(band_path, shpfile, target_resolution_m)
        except:
            print(f'Error reading band {band} for tile {tile} ')

        csv_rows = []

        if not cloud_mask_path:
            parcel_cloud_masks = [None] * len(band_raster_per_parcel)

        for parcel_band_raster, parcel_cloud_mask in zip(band_raster_per_parcel, parcel_cloud_masks):
            filtered_band: np.ndarray = filter_band_using_mask(parcel_band_raster, parcel_cloud_mask)
            if np.count_nonzero(filtered_band) == 0:
                continue

            parcel_id: str = parcel_band_raster['properties'][ui.idname]
            row: [str] = [parcel_id] + filtered_band.flatten().tolist()
            csv_rows.append(row)

        if csv_rows:
            tocsv(date,band,csv_rows,tile,projectname)


def maximal_resolution_band_paths(bands: [str], band_root_path: str, max_resolution_m: int):
    candidate_paths = list(filter(lambda p: parse_resolution_from_path(p)
                                            and parse_resolution_from_path(p) >= max_resolution_m
                                            and parse_band_from_path(p) in bands,
                                  glob(f'{band_root_path}/**/*.jp2', recursive=True)))
    candidate_path_resolutions = list(map(parse_resolution_from_path, candidate_paths))
    candidate_path_bands = list(map(parse_band_from_path, candidate_paths))
    candidate_paths_sorted = sorted(list(zip(candidate_path_resolutions, candidate_paths, candidate_path_bands)),
                                    key=lambda t: t[0])
    max_reso_path_by_band = {}
    for candidate in candidate_paths_sorted:
        reso, path, band = candidate
        if band in max_reso_path_by_band:
            continue
        else:
            max_reso_path_by_band[band] = path

    return list(max_reso_path_by_band.values())


def filter_band_using_mask(parcel_band_raster: [dict], parcel_cloud_mask: [dict]):
    parcel_array = parcel_band_raster['properties']['mini_raster_array'].filled(NO_DATA)

    if parcel_cloud_mask:
        cloud_array = parcel_cloud_mask['properties']['mini_raster_array'].filled(INVALID)
        cloud_mask = np.logical_not(sen2cor_binary_transformer(cloud_array))
        filtered_array = parcel_array[cloud_mask]
    else:
        filtered_array = parcel_array

    return filtered_array[np.isfinite(filtered_array)].astype(np.uint16)


def crop_band_raster_per_parcel(band_path:str, shpfile: str, target_resolution_m: int,
                                resampling=rasterio.enums.Resampling.bilinear) -> [dict]:
    band_data, tile_band_resample_transform = \
        resampled_raster_dataset(band_path,
                                 parse_resolution_from_path(band_path) / target_resolution_m,
                                 resampling=resampling)
    try:
        bandwise_zstats = zonal_stats(shpfile,
                                      band_data,
                                      affine=tile_band_resample_transform,
                                      stats=['count', 'nodata'],
                                      band=1,
                                      nodata=-999,
                                      geojson_out=True,
                                      all_touched=False,
                                      raster_out=True)
    except Exception as e:
        print(f'Error extracting polygons from raster "{band_path}" and shp "{shpfile}":')
        traceback.print_exc()
        raise e

    return bandwise_zstats


def expand_raster_paths(raster_path: str, ui: userinput.UserInput):
    if '.jp2' in raster_path:  # single band
        raster_paths = [raster_path]
        cloud_mask_path = ui.cloud_mask_path
    else:  # all eligible bands in a SAFE dir
        assert ui.target_resolution_m, 'For multi-band operation you must specify a common target resolution.'
        raster_paths = maximal_resolution_band_paths(BANDS, raster_path, ui.target_resolution_m)
        cloud_mask_path = safe_cloud_mask_path(raster_path)

    return cloud_mask_path, raster_paths


def resampled_raster_dataset(raster_path, scaling_factor, resampling=rasterio.enums.Resampling.bilinear):
    try:
        with rasterio.open(raster_path) as dataset:
            raster_data = dataset.read(1,
                                       out_shape=(
                                           dataset.count,
                                           int(dataset.height * scaling_factor),
                                           int(dataset.width * scaling_factor)
                                       ),
                                       resampling=resampling)
            resample_transform = \
                dataset.transform * dataset.transform.scale(
                    (dataset.width / raster_data.shape[-1]),
                    (dataset.height / raster_data.shape[-2])
                )
    except Exception as e:
        print(f'Error reading raster file "{raster_path}":')
        traceback.print_exc()
        raise e
    return raster_data, resample_transform


def parse_resolution_from_path(p: str):
    groups = re.match('.*_([0-9]{2})m.*', p)
    return int(groups[1]) if groups else None


def parse_band_from_path(p: str):
    groups = re.match('.*_(B[0-9].).*', p)
    return groups[1] if groups else None


def parse_tile_from_path(p: str):
    groups = re.match('.*T([0-9]{2}[A-Z]{3}).*', p)
    return groups[1] if groups else None


def parse_date_from_path(rasterpath: str):
    return os.path.split(rasterpath)[-1].split('_')[1][:8]


def safe_cloud_mask_path(safe_root: str):
    return glob(f'{safe_root}/**/*_SCL_20m.jp2', recursive=True)[0]


def array_value_in_one_of(arr: np.ndarray, vals: list):
    return functools.reduce(lambda acc, class_ix: np.logical_or(acc,arr == class_ix), vals, False)


def sen2cor_binary_transformer(array_raw):
    return array_value_in_one_of(array_raw, FILTER_OUT_SEN2COR_CLASSES)


def tocsv(date,band,myarray,tile,projectname):
    csvfile = os.path.join(projectname,'array_'+tile + '_' + date +'_'+ band+'.csv')
    with open(csvfile, "w", newline='') as f:
        writer = csv.writer(f, lineterminator=os.linesep)
        writer.writerows(myarray)


def extractmeta(bandtif, parcelID, mydate, count, nodata, projectname, band, tile):

    #(parcel_ID, year, day-of-year, name of the file (tile), mission ID (SA|SB), count)

    #band and tile could be gotten from bandtif
     
    metadatacsv =  os.path.join(projectname,'meta_'+tile + '_' + mydate +'_'+ band+'.csv')

    mycolumns = ['parcelID','year','DOY','tilefilename','missionID','count', 'nodata']

    if not os.path.exists(metadatacsv): # write the header
        with open(metadatacsv,'w') as csvfile:
            writer = csv.writer(csvfile, delimiter=',')
            writer.writerow(mycolumns)
    
    year = mydate[0:4]

    dateobj = datetime.strptime(mydate, '%Y%m%d')
    doy = (dateobj - datetime(dateobj.year, 1, 1)).days + 1

    bandtif = bandtif.split('/')[-6]
    tilefilename = ('_').join(bandtif.split('_')[0:6])
    
    missionID = bandtif.split('_')[0]

    onerow = [parcelID, year, doy, tilefilename, missionID, count, nodata]

    with open(metadatacsv,'a') as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        writer.writerow(onerow)


if __name__ == "__main__":
    main()
