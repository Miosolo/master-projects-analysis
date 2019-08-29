# %%
from math import ceil
import threading
from queue import Queue
import requests
import pandas as pd
import time
import random

# Get the program records
# %%
# import csv ranking
rankingDF = pd.read_csv('export/rankings.csv').set_index('ucode').apply(lambda col: col.astype('Int64')
                                                                        if col.dtype == 'float64' else col)

# %%
def getPrograms(discCode, page, outQ, topOnly = True):
  """func to get programs online in a thread"""
  if page % workers == 0:
    print('disc', discCode, 'page', page)

  # query and receive json
  respRaw = requests.get(url='https://search.prtl.co/2018-07-23/',
                         params={'q': 'di-{}|en-3098|lv-master|de-fulltime|tc-USD'.format(discCode), 'start': page*10})
  if respRaw.status_code != 200:
    outQ.put(None)
    return

  records = []  # to return
  for prog in respRaw.json():
    # not in TOP
    if prog['organisation_id'] not in topUnivs:
      continue

    rec = []
    # record = [univ, ucode, country, city,
    # program, pcode, degree, fee, duration, summary]
    try:  # get info
      rec.append(prog['organisation'])
      rec.append(prog['organisation_id'])
      rec.append(prog['venues'][0]['country'])
      rec.append(prog['venues'][0]['city'])
      rec.append(prog['title'])
      rec.append(prog['id'])
      rec.append(prog['degree'])

      if prog['tuition_fee']['unit'] != 'year':
        raise AssertionError
      else:
        rec.append(prog['tuition_fee']['value'])

      if prog['fulltime_duration']['unit'] == 'year':
        rec.append(prog['fulltime_duration']['value'] * 12)
      elif prog['fulltime_duration']['unit'] == 'month':
        rec.append(prog['fulltime_duration']['value'])
      elif prog['fulltime_duration']['unit'] == 'day':
        rec.append(prog['fulltime_duration']['value']/30)
      else:
        raise AssertionError

      rec.append(prog['summary'])
    except:
      # print(prog)  # show in log
      continue
    records.append(rec)

  if len(records) == 0:
    outQ.put(None)
  else:
    outQ.put(records)


# %%
# collect programs from mastersportal.com

disciplineDict = {
    24: 'Computer Science & IT',
    23: 'Business & Management',
    7: 'Engineering & Technology',
    11: 'Natural Sciences & Mathematics',
    13: 'Social Sciences'
}
topUnivs = rankingDF.index.tolist() # ucodes
programDF = pd.DataFrame()

#%%
programTable = []
for discCode, discipline in disciplineDict.items():
  # get total numbers
  testReq = requests.get(url='https://search.prtl.co/2018-07-23/',
                         params={'q': 'di-{}|en-1|lv-master|de-fulltime|tc-USD'.format(discCode)})
  totalPages = int(testReq.headers['total']) 
  workers = min(totalPages, 80)
  listenQueue = Queue(workers)
  
  # init first chunk of threads
  pageCnt = 0
  for i in range(workers):
    threading._start_new_thread(getPrograms, (discCode, pageCnt, listenQueue))
    pageCnt += 1
    time.sleep(random.random()/10) # make it a stream
  
  # as consumer
  while pageCnt < totalPages:
    try:
      newTable = listenQueue.get(timeout=10)
    except:
      print('err: timeout')
    if newTable is not None:
      programTable.extend(newTable)
    # reproduce a worker
    time.sleep(random.random()/10)
    threading._start_new_thread(getPrograms, (discCode, pageCnt, listenQueue))
    pageCnt += 1
  
  # [univ, ucode, country, city,
    # program, pcode, degree, duration, fee, summary]
  # merge into the dataframe
  thisDF = pd.DataFrame(programTable)
  thisDF.columns = ['University', 'ucode', 'Country', 'City', 
                    'Program', 'pcode', 'Degree', 'Fee/USD',
                    'Duration/month','Summary']
  thisDF['Discipline'] = discipline
  programDF = pd.concat([programDF, thisDF])

#%%
# export 
with pd.HDFStore('export/top150.h5', 'w') as f:
  f['data'] = programDF