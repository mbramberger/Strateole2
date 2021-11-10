#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 12 04:48:28 2021

@author: martina
"""
  
from pandas import read_csv
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
from matplotlib.widgets import Slider, Button


  
FLOATS_csv_dir = "/Users/martina/Strateole2/FLOATS/FLOATS_C1_52/" # dir where where masterfile is
csv_file = FLOATS_csv_dir+'FLOATS_Raman_Master.csv' 

#  read data 
df = read_csv(csv_file,skiprows=2)
n_profs = len(df)
n_vals = len(df.columns)
vals = np.empty([n_profs,n_vals])
for i in range(n_profs):
   vals[i,:] = df.values[i]

res = vals[:,2]   #spatial resolution
stokes = vals[:,9:9+1850]
antistokes = vals[:,1859:3709]


# get time of different profiles
timestamp = vals[:,0]
year = np.empty(n_profs)
month = np.empty(n_profs)
day = np.empty(n_profs)
dates = [''] * n_profs
for ip in range(n_profs): 
    dt_time = datetime.fromtimestamp(timestamp[ip])
    year[ip] = dt_time.year
    month[ip] = dt_time.month
    day[ip] = dt_time.day
    dates[ip] = dt_time.strftime("%m/%d/%Y")
  

# distance along line
dist = np.arange(0,len(stokes[0,:]))

stokes[stokes == 0] = np.nan
antistokes[antistokes == 0] = np.nan

# plot the scans (start out with the first profile in the file as default)
fig1, ax1 = plt.subplots()
line, = plt.plot(dist,stokes[0,:],'-r*', markersize=2, markeredgewidth=0, color='crimson', label = 'Stokes' )
line1, = plt.plot(dist,antistokes[0,:],'-r*', markersize=2, markeredgewidth=0, color='lightseagreen', label = 'Antistokes')
ax1.set_xlabel('Distance from Laser [m]')
ax1.set_ylabel('Raman Signal')
ax1.legend()
# annotation = 'Date: ' + sel_date
# text = ax1.text(0.6,0.7,annotation, transform=ax1.transAxes)
date_time = datetime.fromtimestamp(timestamp[0])
d = date_time.strftime("%m/%d/%Y, %H:%M:%S")
ax1.set_title("Raman Signal at: "+d)
axcolor = 'lightgoldenrodyellow'
ax1.margins(x=0)

# adjust the main plot to make room for the sliders
plt.subplots_adjust(bottom=0.25)

# Make a horizontal slider to control which szd we plot.
axszd = plt.axes([0.25, 0.1, 0.65, 0.03], facecolor=axcolor)
szd_slider = Slider(
    ax=axszd,
    label='Profile Number',
    valmin=0,
    valmax=n_profs,
    valinit=0,
    valfmt="%i"
)

# The function to be called anytime a slider's value changes
def update(val):
    print('Plotting Scan: ' + str(int(szd_slider.val)))
    # annotation = 'Date: ' + sel_date
    # text.set_text(annotation)
    date_time = datetime.fromtimestamp(timestamp[int(szd_slider.val)])
    d = date_time.strftime("%m/%d/%Y, %H:%M:%S")
    ax1.set_title("Raman Signal at: "+d)
    line.set_ydata(stokes[int(szd_slider.val),:])
    line1.set_ydata(antistokes[int(szd_slider.val),:])
    fig1.canvas.draw_idle()

# register the update function with each slider
szd_slider.on_changed(update)
#amp_slider.on_changed(update)

# Create a `matplotlib.widgets.Button` to reset the sliders to initial values.
nextax = plt.axes([0.8, 0.025, 0.1, 0.04])
button_next = Button(nextax, 'Next', color=axcolor, hovercolor='0.975')
prevax = plt.axes([0.68, 0.025, 0.1, 0.04])
button_prev = Button(prevax, 'Prev', color=axcolor, hovercolor='0.975')

def b_next(event):
    szd_slider.set_val(szd_slider.val + 1)
    update(szd_slider.val + 1)
def b_prev(event):
    szd_slider.set_val(szd_slider.val - 1)
    update(szd_slider.val - 1)

button_next.on_clicked(b_next)
button_prev.on_clicked(b_prev)


plt.show()
