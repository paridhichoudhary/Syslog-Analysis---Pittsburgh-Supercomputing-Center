import pandas as pd
import csv,re,os
import datetime

listDocs = ['/'+x for x in os.listdir() if x.startswith('br') or x.startswith('r')]
listDir = []
for doc in listDocs:
    if os.path.isdir('.'+doc):
        listDir.append('.'+doc)
for dir in listDir:
    for filename in os.listdir(dir):
        try:
            if filename.endswith(".txt"):
                count = 0
                df = pd.DataFrame(columns=['tag_no', 'tag_foll_no', 'DateTime', 'filename', 'tag_name', 'logLine_text'])
                datetime = ''
                fileShortName = ''
                pathname = dir+'/'+filename
                pattern = re.compile("<(\d+)>(\d+)\s(.*?)\s(.+)?\.pvt\.bridges.psc.edu\s(.*?)\s[\s-]+(.*)")
                date = re.search(r'(\d+-\d+-\d+)-.*', filename).group(1)
                if date < "2017-07-01":
                    break
                print(filename)
                with open(pathname) as file:
                    for line in file:
                        count+=1
                        res = re.match(pattern,line)
                        df.loc[len(df)] = [res.group(1),res.group(2),res.group(3),res.group(4),res.group(5),res.group(6)]
                        datetime = res.group(3)
                        fileShortName = res.group(4)
                        print(count)
                    df['tag_foll_no']=df['tag_name'].apply(lambda r: r.split(' ')[1] if len(r.split(' '))>1 else '')
                    df['tag_name']=df['tag_name'].apply(lambda r: r.split(' ')[0] if len(r.split(' '))>1 else r)
                    df = df.loc[:,['tag_no','DateTime','filename','tag_name','tag_foll_no','logLine_text']]
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
                    analysisDF.to_csv(fileShortName+"hotWordsCount.csv")
            else:
                continue
        except Exception as e:
            pass

# pattern=re.compile('.*\ ([a-z]+[0-9]+)(?:\.pvt)?\.bridges\.psc\.edu\ kernel - - -  nfs:\ server(.*)not\ responding')
# count=0
# with open("br005 2018-03-02.txt") as file:
#     for line in file:
#         count += 1
#         # if re.match(pattern,line) is not None:
#         if 'nfs' in line:
#             print(line)
# print(count)
#
#
# df = pd.DataFrame(columns=['tag_no','tag_foll_no','DateTime','filename','tag_name','logLine_text'])
# pattern = re.compile("<(\d+)>(\d+)\s(.*?)\s(.+)?\.pvt\.bridges.psc.edu\s(.*?)\s[\s-]+(.*)")
# with open("br005 2018-03-02.txt") as file:
#     for line in file:
#         count+=1
#         res = re.match(pattern,line)
#         print(res.group(1), res.group(2), res.group(3), res.group(4), res.group(5), res.group(6))
#         df.loc[len(df)] = [res.group(1),res.group(2),res.group(3),res.group(4),res.group(5),res.group(6)]
#         print(count)
# df.to_csv("br005 2018-03-02 df.csv",index=False)
#
# df = pd.read_csv("br005 2018-03-02 df.csv")
# df.drop(labels='Unnamed: 0',axis=1,inplace=True)
# df['tag_foll_no']=df['tag_name'].apply(lambda r: r.split(' ')[1] if len(r.split(' '))>1 else '')
# df['tag_name']=df['tag_name'].apply(lambda r: r.split(' ')[0] if len(r.split(' '))>1 else r)
# df = df.loc[:,['tag_no','DateTime','filename','tag_name','tag_foll_no','logLine_text']]
# tag_names = pd.unique(df['tag_name'])
# listHotWords = ['Error','timed out','fail','Failed','Stopped','PERM','PERF','INFO','TEMP','UNKN','PEND','failure','fault','Err','not responding']
# analysisDF = pd.DataFrame(columns=['Tag_name','Error','timed out','fail','Failed','Stopped','PERM','PERF','INFO','TEMP','UNKN','PEND','failure','fault','Err','not responding'])
# analysisDF = analysisDF.append({'Tag_name':'a','Error':0,'timed out':0,'fail':0,'Failed':0,'Stopped':0,'PERM':0,'PERF':0,'INFO':0,'TEMP':0,'UNKN':0,'PEND':0,'failure':0,'fault':0,'Err':0,'not responding':0},ignore_index=True)
# rowCount = 0
# for i in tag_names:
#     tempDF = df.loc[df['tag_name']==i]
#     tempDict = {'Tag_name':i,'Error':0,'timed out':0,'fail':0,'Failed':0,'Stopped':0,'PERM':0,'PERF':0,'INFO':0,'TEMP':0,'UNKN':0,'PEND':0,'failure':0,'fault':0,'Err':0,'not responding':0}
#     for row in tempDF['logLine_text']:
#         for k in analysisDF.columns[1:]:
#             if k.lower() in row.lower():
#                 tempDict[k] +=1
#     analysisDF = analysisDF.append(tempDict, ignore_index=True)
# print(analysisDF.to_string())

