#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Aug  2 16:41:49 2021

@author: martina
"""
import datetime 
from siphon.simplewebservice.wyoming import WyomingUpperAir
from netCDF4 import Dataset
import time
import numpy as np

output_dir = "/Users/martina/Strateole2/FLOATS/Radiosondes/"   #path where the ouput netcdf file is saved

# choose the date for which you want to download the radiosonde data
year = '2021' 
month = '11' 
days = np.arange(1,10)
hours = ['0', '12']

station = "WIII" # Indonesia: 'WIMM' 'WIMG' and 'WIII'

# loop through all days
for iday in days:
    day = str(iday)
    for ihour in hours:
        hour = ihour
        date = datetime.datetime(int(year), int(month), int(day), int(hour))
        date_str = date.strftime("%m/%d/%Y")
        try:
            # get the data from the website
            df = WyomingUpperAir.request_data(date, station)
            print(df.columns)
            print(df.values)
            
            # store the different parameters into variables
            p = df['pressure'].values
            z = df['height'].values
            T = df['temperature'].values
            dewpoint = df['dewpoint'].values
            direction = df['direction'].values
            speed = df['speed'].values #in kt
            u = df['u_wind'].values
            v = df['v_wind'].values
            time_rs = df['time'].values
            lat = df['latitude'].values
            lon = df['longitude'].values
            elevation = df['elevation'].values
            pw = df['pw'].values
            
            # --- write data to netcdf file
            outfile = output_dir+'RS_'+station+'_'+day+month+year+'_'+hour+'0UTC.nc'
            if ihour == '12':
                outfile = output_dir+'RS_'+station+'_'+day+month+year+'_'+hour+'UTC.nc'
            fid = Dataset(outfile, 'w',format='NETCDF4')
            nz = len(p)
            # ==== Dimensions:
            tdim = fid.createDimension('nz',nz)
            
            # ==== Global attributes:
            fid.author = 'Martina Bramberger'
            fid.contact = 'martina@nwra.com'
            fid.institute = 'NWRA'
            fid.date_created = time.strftime("%Y-%m-%d %H:%M")
            fid.model = 'spec_analysis_RACHUTS'
            
            # ==== Data:
            PALL = fid.createVariable('p',p.dtype,('nz'))
            PALL.units = df.units['pressure']
            PALL.standard_name = 'Pressure'
            PALL[:] = p[:]
            
            TALL = fid.createVariable('T',T.dtype,('nz'))
            TALL.units = df.units['temperature']
            TALL.standard_name = 'Temperature'
            TALL[:] = T[:]
            
            DEALL = fid.createVariable('dewpoint',dewpoint.dtype,('nz'))
            DEALL.units = df.units['dewpoint']
            DEALL.standard_name = 'Dewpoint'
            DEALL[:] = dewpoint[:]
            
            DIALL = fid.createVariable('direction',direction.dtype,('nz'))
            DIALL.units = df.units['direction']
            DIALL.standard_name = 'Direction'
            DIALL[:] = direction[:]
            
            SALL = fid.createVariable('speed',speed.dtype,('nz'))
            SALL.units = df.units['speed']
            SALL.standard_name = 'Speed'
            SALL[:] = speed[:]
            
            UALL = fid.createVariable('u',u.dtype,('nz'))
            UALL.units = df.units['u_wind']
            UALL.standard_name = 'Zonal Wind'
            UALL[:] = u[:]
            
            VALL = fid.createVariable('v',v.dtype,('nz'))
            VALL.units = df.units['v_wind']
            VALL.standard_name = 'Meridional Wind'
            VALL[:] = v[:]
            
            LATALL = fid.createVariable('lat',lat.dtype,('nz'))
            LATALL.units = df.units['latitude']
            LATALL.standard_name = 'Latitude'
            LATALL[:] = lat[:]
            
            LONALL = fid.createVariable('lon',lon.dtype,('nz'))
            LONALL.units = df.units['longitude']
            LONALL.standard_name = 'Longitude'
            LONALL[:] = lon[:]
            
            ELALL = fid.createVariable('elevation',elevation.dtype,('nz'))
            ELALL.units = df.units['elevation']
            ELALL.standard_name = 'Elevation'
            ELALL[:] = elevation[:]
            
            PWALL = fid.createVariable('pw',pw.dtype,('nz'))
            PWALL.units = df.units['pw']
            PWALL.standard_name = 'PW'
            PWALL[:] = pw[:]
            
            ZALL = fid.createVariable('z',z.dtype,('nz'))
            ZALL.units = df.units['height']
            ZALL.standard_name = 'Height'
            ZALL[:] = z[:]
            
            YALL = fid.createVariable('time','S30',('nz'))
            YALL.units = 'year-month-day-hh:mm:ss'
            YALL.standard_name = 'Time of profile'
            datain = np.array(time_rs,dtype='S30')
            YALL._Encoding = 'ascii'
            YALL[:] = datain
            
            fid.close()    
            
        except: 
            print('No data for '+date_str+' ' +hour+'UTC' )
            continue
    
    
