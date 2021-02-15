import sys
import os
import time
import datetime
import socket
import re
import signal
import warnings
warnings.filterwarnings("ignore")

from influxdb import InfluxDBClient
from pymu.server import Server
from pymu.client import Client
from pymu.pmuDataFrame import DataFrame
from pymu.pmuLib import *
import pymu.tools as tools

# influxDB config
host = "sensorweb.us"
username = "test"
port = 8086
pwd = "sensorweb"
dbName = "testdb"
measurement = "microPMU"
location = "lab122"
isSSL = True
db_client = InfluxDBClient(host=host, port=port, username=username, password=pwd, database=dbName, ssl=isSSL)

RUNNING = True

def dbWrite(dFrame, value_list):

    ts = datetime.datetime.strptime(dFrame.soc.formatted, '%Y/%m/%d %H:%M:%S.%f')
    values = [int(ts.timestamp()* 1000000000)]
    for i in range(0, len(dFrame.pmus)):
        for j in range(0, len(dFrame.pmus[i].phasors)):
            values.append(dFrame.pmus[i].phasors[j].deg)
            values.append(dFrame.pmus[i].phasors[j].mag)
        values.append(dFrame.pmus[i].freq)
        values.append(dFrame.pmus[i].dfreq)
    
    value_list.append(
        {
            "measurement": measurement,
            "tags":{
                "location": location
            },
            "fields":{
                "L1ang": values[1],
                "L1mag": values[2],
                "L2ang": values[3],
                "L2mag": values[4],
                "L3ang": values[5],
                "L3mag": values[6],
                "C1ang": values[7],
                "C1mag": values[8],
                "C2ang": values[9],
                "C2mag": values[10],
                "C3ang": values[11],
                "C3mag": values[12],
                "Freq": values[13],
                "Rocof": values[14],
            },
            "time": values[0]
        }
    )

def runPmuToInfluxDB(ip, tcpPort, frameId, udpPort, index=-1, printInfo = True):
    global RUNNING

    print("#{}# Creating Connection\n\t{:<10} {}\n\t{:<10} {}\n\t{:<10} {}".format(index, "IP:", ip, "Port:", tcpPort, "ID Code:", frameId))

    if udpPort > -1:
        print("\t{:<10} {}".format("UDP Port:", udpPort))

    print("----- ----- -----")

    try:
        print("#{}# Reading Config Frame...".format(index)) if printInfo else None
        confFrame = tools.startDataCapture(frameId, ip, tcpPort) # IP address of openPDC
    except Exception as e:
        print("#{}# Exception: {}".format(index, e))
        print("#{}# Config Frame not received...Exiting".format(index))
        sys.exit()

    if confFrame:
        print("#{}# Success!!".format(index)) if printInfo else None
    else:
        print("#{}# Failure!!".format(index)) if printInfo else None

    dataRcvr = None

    if udpPort == -1:
        dataRcvr = Client(ip, tcpPort, "TCP")
    else:
        dataRcvr = Server(udpPort, "UDP")

    dataRcvr.setTimeout(10)

    print("#{}# Starting data collection...\n".format(index))# if printInfo else None
    p = 0
    value_list = [] # writing data 
    buf_size = 120
    milliStart = int(round(time.time() * 1000))
    while RUNNING:
        try:
            # d = tools.getDataSample(dataRcvr)
            dataframe_size, d = tools.getDataSample(dataRcvr)
            if d == '':
                break
            d_list = tools.split_hex_str(d, dataframe_size)
            for i in d_list:
                if p == 0:
                    print("Data Collection Started...")
                dFrame = DataFrame(d, confFrame) # Create dataFrame
                # csvPrint(dFrame, csv_handle)
                dbWrite(dFrame, value_list)
                p += 1
    
            # flush the data to the databases
            if not (p % buf_size):
                db_client.write_points(value_list, time_precision='n', batch_size=10000, protocol='json')
                value_list = []

        except KeyboardInterrupt:
            break
            RUNNING = False
        except socket.timeout:
            print("#{}# Data not available right now...Exiting".format(index))
            break
        except Exception as e:
            print("#{}# Exception: {}".format(index, e))
            break
            
    # Print statistics about processing speed
    milliEnd = int(round(time.time() * 1000))
    if printInfo:
        print("")
        print("##### ##### #####")
        print("Python Stats")
        print("----- ----- -----")
        print("Duration:  ", (milliEnd - milliStart)/1000, "s")
        print("Total Pkts:", p);
        print("Pkts/Sec:  ", p/((milliEnd - milliStart)/1000))
        print("##### ##### #####")
    dataRcvr.stop()

if __name__ == "__main__":
    RUNNING = True
    if len(sys.argv) < 4 or len(sys.argv) > 5:
        print("Usage: python {} <ip> <tcpPort> <frameId> <optional:udpPort>".format(__file__))
        sys.exit()

    ip = sys.argv[1]
    tcpPort = int(sys.argv[2])
    frameId = int(sys.argv[3])
    udpPort = -1
    if len(sys.argv) == 5:
        udpPort = int(sys.argv[4])

    runPmuToInfluxDB(ip, tcpPort, frameId, udpPort, "")