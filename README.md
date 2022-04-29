# Scalable crop yield mapping with Sentinel-2 time series and temporal convolutional network (TCN)

This repository includes codes for preprocessing the data from Sentinel-2 L2A product into time series and and ready for the prediction models TCN and random forests (RF).

in python/

- 01-splitshp-shadow.py: ESRI shapefile for polygons (field parcel) is split into subsets (files) by Sentinel-2 granule boundaries.
- 02-pathfinder.py: filepaths to Sentinel-2 bands is searched. Use this if no intentions for cloud-masking.
- 02-safefinder.py: directory paths to Sentinel-2 SAFE directories. Use this if cloud-masking wanted.
- 03-arrayextractor.py: extract pixel values from bands by polygons. Cloud-mask used is safe paths given.
- 04-flatten-temporal.py: flatten the observations into 11-day temporal composites.
- 05-histogramize-shadow.py: calculate histograms for each observation (band).
- 05-medianize.py: calculate median for each observation (band).
- 06-histo2stack.py: stack histograms from separate files into one file.
- 06-median2stack.py: stack medians from separate files into one file.
- 07-medianstack2ARD.py: make analysis ready data from medians.
- 07-stack2ARD.py: make analysis ready data from histograms.
- 07C-doyFusion-median.py: if duplicates at day-of-year, merge all observations per day per farm into one (matrix addition)
- 07C-doyFusion.py: if duplicates at day-of-year, merge all observations per day per farm into one (matrix addition)
- 08A-removeDuplicates-parallel.py: remove duplicates, if any, compute marix addition.
- 08B-mergeObservations-parallel.py: merge farms by region
- 08-mergeTarget-parallel.py: merge values with reference to write target y files for training.
- 09-runRF-article-iterate.py: run RF, iterate 10 times for each data set (hard coded)
- 09-runTCN-article-iterate.py: run TCN, iterate 10 times for each data set (hard coded)


