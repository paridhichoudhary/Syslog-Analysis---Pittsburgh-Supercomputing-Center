import pandas as pd
import csv,re,os

origin = '/pylon5/sy5fp1p/ehanna/logs/hosts'

# or x.startswith('r')
listDocs = ['/'+x for x in os.listdir(origin) if x[0:4] in ['r012','r047','r105','r221','r274','r296','r336','r363','r381','r426','r473','r491','r531','r564','r579','r586','r608','r615','r640','r702']]
listDir = []
for doc in listDocs:
    if os.path.isdir(origin+doc):
        listDir.append(origin+doc)

# print(listDir)
for direc in listDir:
    count=0
    for filename in os.listdir(direc):
        if filename.endswith('.pvt.bridges.psc.edu.log'):
            pattern = re.compile('<(\d+)>(\d+)\s(.*?)\s(.+)?\.pvt\.bridges.psc.edu\s(.*?)\s[\s-]+(.*)')
            match = re.search(r'(\d+-\d+-\d+)-.*', filename)
            if match is not None:
                date = match.group(1)
                if (date < '2017-10-01' or date > '2017-11-01') and (date < '2018-02-01' or  date > '2018-03-01'):
                    continue
                count+=1
    #             print(date)
    print(direc + ' ' + str(count))

for direc in listDir:
    direcShortName = direc.split('/pylon5/sy5fp1p/ehanna/logs/hosts/')[1][:5]
    os.makedirs(direcShortName)
    for filename in os.listdir(direc):
        try:
            if filename.endswith('.pvt.bridges.psc.edu.log'):
                count = 0
                df = pd.DataFrame(columns=['tag_no', 'tag_foll_no', 'DateTime', 'filename', 'tag_name', 'logLine_text'])
                datetime = ''
                fileShortName = ''
                pathname = direc+'/'+filename
                pattern = re.compile('<(\d+)>(\d+)\s(.*?)\s(.+)?\.pvt\.bridges.psc.edu\s(.*?)\s[\s-]+(.*)')
                match = re.search(r'(\d+-\d+-\d+)-.*', filename)
                if match is not None:
                    date = match.group(1)
                    if (date < '2017-10-01' or date > '2017-11-01') and (date < '2018-02-01' or  date > '2018-03-01'):
                        continue
                else:
                    continue
                print(filename)
                with open(pathname) as file:
                    for line in file:
                        count+=1
                        res = re.match(pattern,line)
                        if res is not None:
                            df.loc[len(df)] = [res.group(1),res.group(2),res.group(3),res.group(4),res.group(5),res.group(6)]
                            datetime = res.group(3)
                            fileShortName = res.group(4)
                        else:
                            continue
                        print(filename + ' ' + str(count))
                    df['tag_name']=df['tag_name'].apply(lambda r: r.split(' ')[0] if len(r.split(' '))>1 else r)
                    df = df.loc[:,['tag_name','logLine_text']]
                    tag_names = pd.unique(df['tag_name'])
                    listHotWords = ['Error','timed out','fail','Failed','Stopped','PERM','PERF','INFO','TEMP','UNKN','PEND','failure','fault','Err','not responding']
                    analysisDF = pd.DataFrame(
                        columns=['DateTime','filename','Tag_name', 'Error', 'timed out', 'fail', 'Failed', 'Stopped', 'PERM', 'PERF', 'INFO',
                                 'TEMP', 'UNKN', 'PEND', 'failure', 'fault', 'Err', 'not responding'])
                    rowCount = 0
                    for i in tag_names:
                        tempDF = df.loc[df['tag_name']==i]
                        tempDict = {'DateTime':datetime,'filename':fileShortName,'Tag_name':i,'Error':0,'timed out':0,'fail':0,'Failed':0,'Stopped':0,'PERM':0,'PERF':0,'INFO':0,'TEMP':0,'UNKN':0,'PEND':0,'failure':0,'fault':0,'Err':0,'not responding':0}
                        for row in tempDF['logLine_text']:
                            for k in analysisDF.columns[1:]:
                                if k.lower() in row.lower():
                                    tempDict[k] +=1
                        analysisDF = analysisDF.append(tempDict, ignore_index=True)
                    analysisDF.to_csv('./'+direcShortName+'/'+'-'+datetime[:10]+'hotWordsCount.csv',index=False)
            else:
                continue
        except Exception as e:
            print(e)
            continue