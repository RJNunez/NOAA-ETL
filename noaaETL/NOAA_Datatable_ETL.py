#
#   Rolando J. Acosta
#   August 13, 2016
#
#   NOAA ETL process for the data table
#
#

# Imports
import os
import sys
import csv
import json
import zipfile
import time
import datetime
import os.path
import urllib2
import tarfile
import traceback
import optparse
import requests
import psycopg2
import numpy as np
import pandas as pd
import geopandas as gpd
from pandas.io import sql
import pandas.io.sql as psql
from datetime import datetime
from bs4 import BeautifulSoup
from shapely.geometry import Point
from psycopg2.extensions import AsIs
from sqlalchemy import Column, Integer, String
from os.path import isdir, join, normpath, split
from sqlalchemy.ext.declarative import declarative_base
from geopandas import GeoSeries, GeoDataFrame, read_file
from sqlalchemy import create_engine, MetaData, TEXT, Integer, Table, Column, ForeignKey
from sqlalchemy.dialects.postgresql import ARRAY, BIGINT, BIT, \
	BOOLEAN, BYTEA, CHAR, CIDR, DATE, \
	DOUBLE_PRECISION, ENUM, FLOAT, HSTORE, INET, INTEGER, \
	INTERVAL, JSON, JSONB, MACADDR, NUMERIC, OID, REAL, SMALLINT, TEXT, \
	TIME, TIMESTAMP, UUID, VARCHAR, INT4RANGE, INT8RANGE, NUMRANGE, \
	DATERANGE, TSRANGE, TSTZRANGE, TSVECTOR

# GLOBAL VARIABLES

# Missing data
NA_VALUES = ['M', '-', 'T']

# To look for the data
s_with = 'QCLCD'
out_dir = '/scratch/environmentalPICI_ETL/pythonETL/noaaETL/noaaZipFiles'

# Connection
connection = psycopg2.connect(database="chiraglab", user="dbetl", password="exposome", host="chiraglab.cift52l3iihu.us-east-1.rds.amazonaws.com")
cursor = connection.cursor()

# TO DETERMINE WHAT COLUMNS OF THE
# RAW DATA CORRESPONDS TO WHAT GROUP
col_temperature = {'monthly': ['WBAN', 'YearMonthFormatted', 'AvgMaxTemp', 'AvgMinTemp', 'AvgTemp', 'AvgDewPoint', 'AvgWetBulb', 'DepartureMaxTemp', 'DepartureMinTemp', 'DeparturefromNormal', 'Latitude', 'Longitude', 'State'],
                   'daily':   ['WBAN', 'YearMonthDayFormatted', 'Tmax', 'Tmin', 'Tavg', 'DewPoint', 'WetBulb', 'Latitude', 'Longitude', 'State'],
                   'hourly':  ['WBAN', 'YearMonthDayFormatted', 'DryBulbFarenheit', 'WetBulbFarenheit', 'DewPointFarenheit', 'RelativeHumidity', 'Latitude', 'Longitude', 'State']}
meas_temperature = 'Temperature'

col_wind = {'monthly': ['WBAN', 'YearMonthFormatted', 'AvgWindSpeed', 'ResultantWindSpeed', 'ResultantWindDirection', 'Latitude', 'Longitude', 'State'],
            'daily':   ['WBAN', 'YearMonthDayFormatted', 'ResultSpeed', 'ResultDir', 'AvgSpeed', 'Latitude', 'Longitude', 'State'],
            'hourly':   ['WBAN', 'YearMonthDayFormatted', 'WindSpeed', 'WindDirection', 'Latitude', 'Longitude', 'State']}
meas_wind = 'Wind'

col_pressure = {'monthly': ['WBAN', 'YearMonthFormatted', 'MeanStationPressure', 'MeanSeaLevelPressure', 'MaxSeaLevelPressure', 'MinSeaLevelPressure', 'DateMaxSeaLevelPressure', 'DateMinSeaLevelPressure', 'Latitude', 'Longitude', 'State'],
                'daily':   ['WBAN', 'YearMonthDayFormatted', 'StnPressure', 'SeaLevel', 'Latitude', 'Longitude', 'State'],
                'hourly':  ['WBAN', 'YearMonthDayFormatted', 'StationPressure', 'Altimeter', 'PressureTendency', 'Latitude', 'Longitude', 'State']}
meas_pressure = 'Pressure'

col_precipitation = {'monthly': ['WBAN', 'YearMonthFormatted', 'TotalMonthlyPrecip', 'Max24HrPrecip', 'TotalSnowfall', 'Max24HrSnowfall', 'DateMax24HrPrecip', 'DateMax24HrSnowfall', 'DepartureFromNormalPrecip', 'Latitude', 'Longitude', 'State'],
                     'daily':   ['WBAN', 'YearMonthDayFormatted', 'PrecipTotal', 'SnowFall', 'Latitude', 'Longitude', 'State'],
                     'hourly':  ['WBAN', 'YearMonthDayFormatted', 'HourlyPrecip', 'Latitude', 'Longitude', 'State']}
meas_precipitation = 'Precipitation'

# Dictionary for the units and types
# of all the measurements
master_meas = {
'Temperature': {'Monthly': {'Temperature': {'AvgMaxTemp':           {'units': 'Fahrenheit', 'measurement type': 'Temperature'},
                            'AvgMinTemp':           {'units': 'Fahrenheit', 'measurement type': 'Temperature'},
                            'AvgTemp':              {'units': 'Fahrenheit', 'measurement type': 'Temperature'},
                            'AvgDewPoint':          {'units': 'Fahrenheit', 'measurement type': 'Temperature'},
                            'AvgWetBulb':           {'units': 'Fahrenheit', 'measurement type': 'Temperature'},
                            'DepartureMaxTemp':     {'units': 'Fahrenheit', 'measurement type': 'Temperature'},
                            'DepartureMinTemp':     {'units': 'Fahrenheit', 'measurement type': 'Temperature'},
                            'DeparturefromNormal':  {'units': 'Fahrenheit', 'measurement type': 'Temperature'}}},

                'Daily':   {'Temperature': {'Tmax':       {'units': 'Fahrenheit', 'measurement type': 'Temperature'},
                            'Tmin':       {'units': 'Fahrenheit', 'measurement type': 'Temperature'},
                            'Tavg':       {'units': 'Fahrenheit', 'measurement type': 'Temperature'},
                            'DewPoint':   {'units': 'Fahrenheit', 'measurement type': 'Temperature'},
                            'WetBulb':    {'units': 'Fahrenheit', 'measurement type': 'Temperature'}}},

                'Hourly':   {'Temperature': {'DryBulbFarenheit':  {'units': 'Fahrenheit', 'measurement type': 'Temperature'},
                             'WetBulbFarenheit':  {'units': 'Fahrenheit', 'measurement type': 'Temperature'},
                             'DewPointFarenheit': {'units': 'Fahrenheit', 'measurement type': 'Temperature'},
                             'RelativeHumidity':  {'units': 'Percentage', 'measurement type': 'Temperature'}}}},

'Pressure':    {'Monthly':  {'Pressure': {'MeanStationPressure':       {'units': 'Inches of Hg', 'measurement type': 'Pressure'},
                             'MeanSeaLevelPressure':      {'units': 'Inches of Hg', 'measurement type': 'Pressure'},
                             'MaxSeaLevelPressure':       {'units': 'Inches of Hg', 'measurement type': 'Pressure'},
                             'DateMaxSeaLevelPressure':   {'units': 'Day', 'measurement type': 'Pressure'},
                             'DateMinSeaLevelPressure':   {'units': 'Day', 'measurement type': 'Pressure'},
                             'MinSeaLevelPressure':       {'units': 'Inches of Hg', 'measurement type': 'Pressure'}}},

                'Daily':    {'Pressure': {'StnPressure': {'units': 'Inches of Hg', 'measurement type': 'Pressure'},
                             'SeaLevel':    {'units': 'Inches of Hg', 'measurement type': 'Pressure'}}},

                'Hourly':   {'Pressure': {'StationPressure': {'units': 'Inches_of_Hg', 'measurement type': 'Pressure'},
                             'PressureTendency': {'units': 'Categorical', 'measurement type': 'Pressure'},
                             'Altimeter':    {'units': 'Inches_of_Hg', 'measurement type': 'Pressure'}}}},

'Precipitation':    {'Monthly':  {'Precipitation': {'TotalMonthlyPrecip':          {'units': 'Inches', 'measurement type': 'Precipitation'},
                                  'DateMax24HrPrecip':           {'units': 'Day', 'measurement type': 'Precipitation'},
                                  'DateMax24HrSnowFall':         {'units': 'Day', 'measurement type': 'Precipitation'},
                                  'Max24HrPrecip':               {'units': 'Inches', 'measurement type': 'Precipitation'},
                                  'TotalSnowFall':               {'units': 'Inches', 'measurement type': 'Precipitation'},
                                  'Max24HrSnowfall':             {'units': 'Inches', 'measurement type': 'Precipitation'},
                                  'DepartureFromNormalPrecip':   {'units': 'Inches', 'measurement type': 'Precipitation'}}},

                     'Daily':    {'Precipitation': {'SnowFall':    {'units': 'Inches', 'measurement type': 'Precipitation'},
                                  'PrecipTotal': {'units': 'Inches of Hg', 'measurement type': 'Pressure'}}},

                     'Hourly':   {'Precipitatioin': {'HourlyPrecip':{'units': 'Inches', 'measurement type': 'Precipitation'}}}},

'Wind':              {'Monthly': {'WindSpeed': {'ResultantWindSpeed':    {'units': 'MPH', 'measurement type': 'Speed'},
                                  'AvgWindSpeed':          {'units': 'MPH', 'measurement type': 'Speed'}},

                                  'WindDirection': {'ResultantWindDirection':{'units': 'Tens_of_Degrees', 'measurement type': 'Direction'}}},

                      'Daily':   {'WindSpeed': {'ResultSpeed': {'units': 'MPH', 'measurement type': 'Speed'},
                                  'AvgSpeed': {'units': 'MPH', 'measurement type': 'Speed'}},

                                  'WindDirection': {'ResultDir': {'units': 'Tens_of_Degrees', 'measurement type': 'Direction'}}},

                      'Hourly':  {'WindSpeed': {'WindSpeed': {'units': 'MPH', 'measurement type': 'Speed'}},

                                  'WindDirection': {'WindDirection': {'units': 'Tens_of_Degrees', 'measurement type': 'Direction'}}}}}


years = ['2007', '2008', '2009', '2010', '2011', '2012', '2013', '2014', '2015', '2016']
months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
datatable_column_names = ['factid', 'datasource', 'timeframe', 'timeframe_unit','factname', 'measurement','tags']


def getZipArchive(directory, year, month):
	str_form = s_with + year + month + '.zip'
	pathtofile = os.path.join(out_dir, str_form)
	archive = zipfile.ZipFile(pathtofile, 'r')
	return archive

def date_parser(x):
	return datetime.strptime(x, '%Y%m%d')

def station_file(year, month):
	return year + month + 'station.txt'

def daily_file(year, month):
	return year + month + 'daily.txt'

def hourly_file(year, month):
	return year + month + 'hourly.txt'

def monthly_file(year, month):
	return year + month + 'monthly.txt'

def read_station_file_to_df(year, month, archive):
	dataTypes = {'WBAN': str}
	station = pd.read_csv(archive.open(station_file(year, month)), sep="|", dtype=dataTypes)
	return station

def read_daily_file_to_df(year, month, archive):
	dataTypes = {'WBAN':str}
	daily = pd.read_csv(archive.open(daily_file(year, month)), sep=",", dtype=dataTypes, na_values=NA_VALUES, parse_dates={'YearMonthDayFormatted':['YearMonthDay']}, date_parser=date_parser)
	stations = read_station_file_to_df(year,month, archive)
	merge_daily = stations.merge(daily, left_on = 'WBAN', right_on = 'WBAN', how = 'inner')
	return merge_daily

def read_hourly_file_to_df(year, month, archive):
	dataTypes = {'WBAN':str, 'StationType':float, 'RelativeHumidityFlag':str, 'WindSpeedFlag':str, 'Time':str}
	hourly = pd.read_csv(archive.open(hourly_file(year, month)), sep=",", dtype=dataTypes, na_values=NA_VALUES, parse_dates={'YearMonthDayFormatted':['Date']}, date_parser=date_parser)
	stations = read_station_file_to_df(year, month, archive)
	merge_hourly = stations.merge(hourly, left_on = 'WBAN', right_on = 'WBAN', how = 'inner')
	return merge_hourly

def read_monthly_file_to_df(year, month, archive):
	dataTypes = {'WBAN':str}
	def mth_date_parser(x):
		return datetime.strptime(x, '%Y%m')
	monthly = pd.read_csv(archive.open(monthly_file(year, month)), sep=",", dtype=dataTypes, na_values=NA_VALUES,  parse_dates={'YearMonthFormatted':['YearMonth']}, date_parser=mth_date_parser)
	stations = read_station_file_to_df(year, month, archive)
	merge_monthly = stations.merge(monthly, left_on = 'WBAN', right_on = 'WBAN', how = 'inner')
	return merge_monthly

# It looks for the zip files in the out_dir directory
# Parses each file by time frame and type (e.g., month-Temperature or day-Pressure)
# The function returns a list of two lists. The first one is a list of names (e.g., Temperature-2009-10-day-Table)
# The second one is a list of data frames that corresponds to the names of the same position in the first list.
def getTable(out_dir, years, months, measurement, col_dic):
	data_tables = []
	data_tables_names = []
	master_list = []
	for year in years:
		for month in months:
			try:
				archive = getZipArchive(out_dir, year, month)
				file_names = archive.namelist()
				print 'Parsing: '+year+'-'+month+' '+measurement+' data'
	  			for name in file_names:
	  				if name[6:] == 'monthly.txt':
	  					print 'Parsing monthly data'
	  					time_frame = 'monthly'
	  					col_names = col_dic[time_frame]
	  					df = read_monthly_file_to_df(year, month, archive)
						df_temp = df[col_names]
						data_tables.append(df_temp)
						data_tables_names.append(measurement+'-'+year+'.'+month+'_'+time_frame+'Table')
	  				if name[6:] == 'daily.txt':
	  					print 'Parsing daily data'
	 					time_frame = 'daily'
	 					col_names = col_dic[time_frame]
	 					df = read_daily_file_to_df(year, month, archive)
						df_temp = df[col_names]
						data_tables.append(df_temp)
						data_tables_names.append(measurement+'-'+year+'.'+month+'_'+time_frame+'Table')
#	 				elif name[6:] == 'hourly.txt':
#	  					print 'Parsing hourly data'
#	 					time_frame = 'hourly'
#	 					col_names = col_dic[time_frame]
#	 					df = read_hourly_file_to_df(year, month, archive)
#						df_temp = df[col_names]
#						data_tables.append(df_temp)
#						data_tables_names.append(measurement+'-'+year+'.'+month+'_'+time_frame+'Table')
			except:
				 print  'ERROR: '+year+'-'+month+' ' +measurement +' data does not exist'
		master_list = [data_tables_names, data_tables]
	return master_list

# Parsing and formating of the data for the data table
def uploadToDataTable(master_list):
	names = master_list[0]
	for name in names:
		print 'Uploading '+name+' data to data table'
		pos_ = name.index('_')
		pos = name.index('-')
		datatable = pd.DataFrame(columns = datatable_column_names)
		if name[pos_+1:] == 'monthlyTable':
			datatable['factid'] = ['month_' + name[:pos] + '_NOAA']
			datatable['datasource'] = ['NOAA']
			datatable['timeframe'] = ['month']
			datatable['timeframe_unit'] = [1]
			datatable['factname'] = [name[:pos]]
			datatable['measurement'] = json.dumps(master_meas[name[:pos]]['Monthly'])
			datatable['tags'] = [tags[name[:pos]]['Monthly']]
			writeToDataTable(datatable)
		elif name[pos_+1:] == 'dailyTable':
			datatable['factid'] = ['day_' + name[:pos] + '_NOAA']
			datatable['datasource'] = ['NOAA']
			datatable['timeframe'] = ['day']
			datatable['timeframe_unit'] = [1]
			datatable['factname'] = [name[:pos]]
			datatable['measurement'] = json.dumps(master_meas[name[:pos]]['Daily'])
			datatable['tags'] = [tags[name[:pos]]['Daily']]
			writeToDataTable(datatable)
		elif name[pos_+1:] == 'hourlyTable':
			datatable['factid'] = ['hour_' + name[:pos] + '_NOAA']
			datatable['datasource'] = ['NOAA']
			datatable['timeframe'] = ['hour']
			datatable['timeframe_unit'] = [1]
			datatable['factname'] = [name[:pos]]
			datatable['measurement'] = json.dumps(master_meas[name[:pos]]['Hourly'])
			datatable['tags'] = [tags[name[:pos]]['Hourly']]
#			writeToDataTable(datatable)
	return

# Writes to the data table
def writeToDataTable(data_table):
    data_database = psql.read_frame("SELECT factid from exposome_pici.datatable WHERE datasource ='NOAA'", con = connection)
    key_local = str(data_table['factid'].to_string()[4:].replace(' ',''))
    unique_factid = np.unique(data_database.factid)
    if key_local not in unique_factid:
        cursor.execute("INSERT INTO exposome_pici.datatable (factid, datasource, timeframe, timeframe_unit, factname, measurement, tags)" + "VALUES (%s, %s, %s, %s, %s, %s, %s)",
        (data_table.factid.loc[0], data_table.datasource.loc[0], data_table.timeframe.loc[0], data_table.timeframe_unit.loc[0],
        data_table.factname.loc[0], data_table.measurement.loc[0], list(data_table.tags.loc[0])))
        connection.commit()
    return

# DataTable - ETL Temperature
temperature_tables = getTable(out_dir, years, months, meas_temperature, col_temperature)
uploadToDataTable(temperature_tables)

# DataTable - ETL Wind
wind_tables = getTable(out_dir, years, months, meas_wind, col_wind)
uploadToDataTable(wind_tables)

# DataTable - ETL Pressure
pressure_tables = getTable(out_dir, years, months, meas_pressure, col_pressure)
uploadToDataTable(pressure_tables)

# DataTable - ETL Precipitation
precipitation_tables = getTable(out_dir, years, months, meas_precipitation, col_precipitation)
uploadToDataTable(precipitation_tables)
