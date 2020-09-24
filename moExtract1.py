import struct
import os
import fnmatch
from datetime import datetime

pDirs = [
    "/Users/cmaddux/myVMs/vbshare/CNSdata/Penn/patients/Patient11a"
#    "/Users/cmaddux/myVMs/vbshare/CNSdata/Penn/patients/Patient21"
#    "/Users/cmaddux/myVMs/vbshare/CNSdata/Penn/patients/Patient62"
]

CSV_FMT='%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s'

indexFmt = '=QQIIBBH'
indexLen = struct.calcsize(indexFmt)
indexUnpack = struct.Struct(indexFmt).unpack_from
dataFmt = '=f'
dataLen = struct.calcsize(dataFmt)

indexTable = []

def loadIndexTable(indexFile):
    indexTable.clear()
    with open(indexFile, "rb") as i:
        while True:
            data = i.read(indexLen)
            if not data: break
            s = list(indexUnpack(data))
            indexTable.append(s)

    for x in range(len(indexTable) - 1):
        indexTable[x].append(indexTable[x + 1][0] - indexTable[x][0])

    lastlen = int(dataFileLen / indexTable[len(indexTable) - 1][4])
    indexTable[len(indexTable) - 1].append(lastlen - indexTable[len(indexTable) - 1][0])

    for x in range(len(indexTable)):
        t = datetime.utcfromtimestamp(indexTable[x][1] / 1000000)
        indexTable[x].append(str(t))
        print(indexTable[x])
    i.close()
    return

def processDataFile(dataFile):
    modality = dataFile.split("/")[len(dataFile.split("/")) - 1].split(",")[0]
    location = dataFile.split("/")[len(dataFile.split("/")) - 1].split(",")[1]
    print("Processing mod,loc: " + modality + "," + location)
    dfile = open(dataFile, "rb")
    for x in range(len(indexTable)):
        fileOffset = indexTable[x][0] * dataLen
        dfile.seek(fileOffset)
        runCount = indexTable[x][7]
        data = dfile.read(dataLen * runCount)
        ts = indexTable[x][1]
        dataIndex = 0
        for y in range(runCount):
            s = struct.unpack_from(dataFmt, data, dataIndex)
            print(CSV_FMT % (modality, location,
                             indexTable[x][0],
                             indexTable[x][1],
                             indexTable[x][3],
                             indexTable[x][5],
                             indexTable[x][7],
                             indexTable[x][8],
                             ts, datetime.utcfromtimestamp(ts / 1000000),
                             float(s[0]),
                             10),
                  file=ofile)
            ts += (indexTable[x][3])
            dataIndex += dataLen
    dfile.close()
    return

#############################################################
### main
#############################################################

if __name__ == '__main__':
    for d in pDirs:
        print("Processing directory: " + d)
        fList = os.listdir(d)
        pattern = "NBP,*,Numeric,*,index"

        outFile = d.split("/")[len(d.split("/")) - 1].lower() + ".csv"
        print("outFile: " + outFile)
        ofile = open(outFile, "w")
        print(CSV_FMT % ("Modality", "Location", "SegOffset", "SegEpoch", "SegInterval", "CheckSum", "SampLen",
                          "SegBegTS", "SampEpoch", "SampTS", "Value", "10"), file=ofile)

        for f in fList:
            if fnmatch.fnmatch(f, pattern):
                indexFile = d + "/" + f
                print("Processing Index File: " + indexFile)
                dataFile = indexFile[:-5] + "data"
                print("Datafile: " + dataFile)
                indexFileLen = os.path.getsize(indexFile)
                dataFileLen = os.path.getsize(dataFile)
                loadIndexTable(indexFile)
                processDataFile(dataFile)

        ofile.close()
