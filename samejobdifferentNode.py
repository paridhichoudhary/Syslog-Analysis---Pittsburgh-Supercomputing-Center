import csv


file = open('SameJobDiffNode.txt')

text = ''
count=0
for line in file:
    text+=line
jobDetails = line.split('||')
for job in jobDetails:
    if 'True' in job and 'False' in job:
        print(job)
# print(count)
