import serial
import time
import datetime
import os
import numpy
import csv

com_port = 'COM9'

try:
        ser = serial.Serial(
                port = (com_port),\
                baudrate = 115200,\
                parity = serial.PARITY_NONE,\
                stopbits = serial.STOPBITS_ONE,\
                bytesize = serial.EIGHTBITS,\
                timeout = None)
        print ('connected to port {}'.format(com_port))
        
except serial.SerialException:
        print ("ERROR: Could Not Open COM %r!" % (com_port))


position = ('ubx_data')

OutputFP = os.path.dirname(os.path.realpath(__file__))+"\\data"
if not os.path.exists(OutputFP):
    os.makedirs(OutputFP)
    
fileraw = '{}\\{}.csv'.format(OutputFP,position)


data = "site,fix,lat,lon,hacc,vacc,msl,sat_num,temp,volt,ts\n"
fraw = open(fileraw, 'a')   #append
fraw.write(data)
fraw.close()


while True:
    data = ser.readline().decode()
    data = "{}".format(data)

    data = data.translate(str.maketrans('','','\r'))

    fraw = open(fileraw, 'a')
    fraw.write(data)
    fraw.close()   
        
    print(data)
     
      

 