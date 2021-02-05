import pymu.tools as tools
from pymu.pmuDataFrame import DataFrame
from pymu.client import Client


frameId = 1
ipaddr = '192.168.137.79'
tcpPort = 4712

confFrame = tools.startDataCapture(frameId, ipaddr, tcpPort)

cli = Client(ipaddr, tcpPort, "TCP")
dataSample = tools.getDataSample(cli)
dataFrame = DataFrame(dataSample, confFrame)

print('hello')