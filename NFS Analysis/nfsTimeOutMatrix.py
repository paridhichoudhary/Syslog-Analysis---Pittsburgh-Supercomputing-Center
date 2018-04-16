
# coding: utf-8

# In[13]:


import pandas as pd
import numpy as np
import re,os
import datetime


# In[14]:


origin= '/pylon5/sy5fp1p/ehanna/logs/hosts/'


# In[15]:


pattern = re.compile('(\d+-\d+-\d+)T(\d+:\d+:\d+)-(\d+:\d+)\s(.*?)\.pvt')


# In[53]:


nfsTimeouts = []
brNodePath = origin + 'br005.pvt.bridges.psc.edu/'
date = '2018-01-22'
startTime = datetime.datetime.now()
for direc in os.listdir(origin):
    if (direc.startswith('br') or direc.startswith('r')) and direc.endswith('.pvt.bridges.psc.edu'):
        fileName = date+'-'+direc+'.log'
        if fileName in os.listdir(origin+direc+'/'):
            with open(origin+direc+'/'+fileName) as file:
                for line in file:
                    if 'timed out' in line and 'nfs: server ' in line:
                        match = re.search(pattern,line)
                        if match is not None:
                            date = match.group(1)
                            time = match.group(2)
                            Zone = match.group(3)
                            node = match.group(4)
                            nfsTimeouts.append(date+'|'+time+'|'+node)
endTime = datetime.datetime.now()
print(endTime-startTime)


# In[16]:


sortednfsTimeouts = sorted(nfsTimeouts)


# In[54]:


with open(origin+'/br006.pvt.bridges.psc.edu'+'/2018-01-22-br006.pvt.bridges.psc.edu.log') as file:
    count_NR = 0
    count_OK = 0
    count_TO = 0
    for line in file:
        if 'nfs' in line and 'not responding' in line:
            count_NR+=1
        if 'nfs' in line and 'timed out' in line:
            count_TO+=1
        if 'nfs' in line and 'OK' in line:
            count_OK+=1            
    print(count_NR)
    print(count_TO)
    print(count_OK)


# In[20]:


uniqueTimestamps = []
for x in sortednfsTimeouts:
    t = x.split('|')[1]
    t = t.split(':')[0]+':'+t.split(':')[1] + ':'+'00'
    if t not in uniqueTimestamps:
        uniqueTimestamps.append(t)


# In[22]:


allNodes = []
for x in sortednfsTimeouts:
    t = x.split('|')[2]
    if t not in allNodes:
        allNodes.append(t)


# In[55]:


nfsMatrix = pd.DataFrame(0,index=uniqueTimestamps, columns=allNodes)


# In[56]:


currentRow = 0
i=0
startTime = datetime.datetime.now()
while i <len(nfsMatrix):
    if currentRow <len(sortednfsTimeouts):
        checkTimestamp = nfsMatrix.index[i]
        checkTimestampDT = datetime.datetime.strptime(checkTimestamp, '%H:%M:%S')
        currentTimeStamp = sortednfsTimeouts[currentRow].split('|')[1]
        currentTimeStampDT = datetime.datetime.strptime(currentTimeStamp, '%H:%M:%S')
        nextTimeStampDT = checkTimestampDT + datetime.timedelta(minutes = 1)
        currentNode = sortednfsTimeouts[currentRow].split('|')[2]
        while currentTimeStampDT>=checkTimestampDT and currentTimeStampDT<nextTimeStampDT:
            nfsMatrix.loc[checkTimestamp,currentNode] +=1
            currentRow +=1
            if currentRow<len(sortednfsTimeouts):
                currentTimeStamp = sortednfsTimeouts[currentRow].split('|')[1]
                currentTimeStampDT = datetime.datetime.strptime(currentTimeStamp, '%H:%M:%S')
                currentNode = sortednfsTimeouts[currentRow].split('|')[2]
            else:
                break
        i+=1
    else:
        break
endTime = datetime.datetime.now()
print(endTime-startTime)


# In[58]:


nfsMatrix;


# In[59]:


nfsMatrix.to_csv("NFSErrorCount-2018-01-22.csv")

