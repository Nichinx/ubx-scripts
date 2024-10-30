import serial
import time
import datetime
import os
import numpy
import csv

com_port = 'COM34'

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

mohon_no = input('Mohon No.: ')
file_name = (f'UPMHN_{mohon_no}')

OutputFP = os.path.dirname(os.path.realpath(__file__))+"\\data"
if not os.path.exists(OutputFP):
    os.makedirs(OutputFP)
    
fileraw = '{}\\{}.csv'.format(OutputFP,file_name)


# data = "site,fix_type,latitude,longitude,hacc,vacc,msl,sat_num,temp,volt,ts\n"
# fraw = open(fileraw, 'a')   #append
# fraw.write(data)
# fraw.close()


# while True:
#     data = ser.readline().decode()
#     data = "{}".format(data)

#     data = data.translate(str.maketrans('','','\r'))

#     fraw = open(fileraw, 'a')
#     fraw.write(data)
#     fraw.close()   
        
#     print(data)
     
      
if not os.path.isfile(fileraw):
    data = "site,fix_type,latitude,longitude,hacc,vacc,msl,sat_num,temp,volt,ts\n"
    with open(fileraw, 'a') as fraw:
        fraw.write(data)

while True:
    data = ser.readline().decode().strip()

    # Only process lines that start with ">>"
    if data.startswith(">>"):
        data = data[2:]
        with open(fileraw, 'a') as fraw:
            fraw.write(data + '\n')
        
        print(data)
