# %%
from math import ceil
import threading
from queue import Queue
from bs4 import BeautifulSoup
import requests
import pandas as pd
import time
import random

# Get the program records
# %%
# import csv ranking
rankingDF = pd.read_hdf('export/top150.h5', key='ranking')

# %%
def getPrograms(discCode, page, outQ, topOnly = True):
  '''func to get programs online in a thread'''
  if page % workers == 0:
    print('disc', discCode, 'page', page)

  # query and receive json
  respRaw = requests.get(url='https://search.prtl.co/2018-07-23/',
                         params={'q': 'di-{}|en-3098|lv-master|de-fulltime|tc-USD'.format(discCode), 'start': page*10})
  if respRaw.status_code != 200:
    print('network error:', respRaw.content.decode())
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
    try:
      rec.append(prog['organisation'])
      rec.append(prog['organisation_id'])
      try:
        rec.append(prog['venues'][0]['country'])
      except:
        rec.append('')
      try:
        rec.append(prog['venues'][0]['city'])
      except:
        rec.append('')
      rec.append(prog['title'])
      rec.append(prog['id'])
      rec.append(prog['degree'])
      
      try:
        if prog['fulltime_duration']['unit'] == 'year':
          dur = prog['fulltime_duration']['value'] * 12
        elif prog['fulltime_duration']['unit'] == 'month':
          dur = prog['fulltime_duration']['value']
        elif prog['fulltime_duration']['unit'] == 'day':
          dur = prog['fulltime_duration']['value']/30
        else:
          raise AssertionError
      except:
        dur = ''
      
      try:
        if prog['tuition_fee']['unit'] == 'year':
          fee = prog['tuition_fee']['value']
        elif prog['tuition_fee']['unit'] == 'month':
          fee = prog['tuition_fee']['value'] * 12
        elif prog['tuition_fee']['unit'] == 'full':
          fee = prog['tuition_fee']['value'] / dur # may raise ArithmeticError
        else:
          raise AssertionError
      except:
        fee = ''
      rec.append(fee)
      rec.append(dur)
      rec.append(prog['summary'])
    except Exception as e:
      print('{}: {}, {}'.format(repr(e), prog['organisation'], prog['title']))  # show in log
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
topUnivs = rankingDF.index.astype('int').tolist() # ucodes
programDF = pd.DataFrame()

#%%
programTable = []
for discCode, discipline in disciplineDict.items():
  # get total numbers
  testReq = requests.get(url='https://search.prtl.co/2018-07-23/',
                         params={'q': 'di-{}|en-1|lv-master|de-fulltime|tc-USD'.format(discCode)})
  totalPages = min(ceil(int(testReq.headers['total'])/10), 1000) # website limit
  # totalPages = 100 # for test
  workers = min(totalPages, 100)
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
programDF.to_hdf('export/top150.h5', 'program')

# Program detail spider
# objectives: overview/ about -> NLP; course outline; language requirements; general/ academic reqirements; 
# living costs; funding

#%%
def getProgramDetails(pcode, outQ):
  '''func to get program details from HTML in a thread'''
  page = requests.get('https://www.mastersportal.com/studies/{}'.format(pcode))
  if page.status_code != 200:
    print('network error for {}'.format(pcode))
    outQ.put(None)
    return
  
  soup = BeautifulSoup(page.content)
  # [pcode, about, outline[], language{}, requirements[], livingCost{}]
  record = [pcode]
  try: # about
    record.append(soup.find('section', attrs={'id': 'StudyDescription'}).p.get_text())
  except:
    record.append('')
  
  try: # outline[]
    outline = []
    for li in soup.find('article', attrs={'id': 'StudyContents'}).find_all('li'):
      outline.append(li.get_text())
    record.append(outline)
  except:
    record.append([])
  
  langReq = {}
  try: # IELTS
    iSec = soup.find('li', attrs={'class': 'SegmentedControlItem js-SegmentedControlItem',
                                      'data-segment-id': 'IELTS'})
    ieltsScore = float(iSec.find('div', attrs={'class': 'Score js-Score'}).string)
    langReq['IELTS'] = ieltsScore
  except:
    pass
  try: # TOEFL IBT
    ibtSec = soup.find('li', attrs={'class': 'SegmentedControlItem js-SegmentedControlItem',
                                      'data-segment-id': 'TOEFL IBT'})
    ibtScore = float(ibtSec.find('div', attrs={'class': 'Score js-Score'}).string)
    langReq['TOFEL IBT'] = ibtScore
  except:
    pass
  try: # TOFEL PBT
    pbtSec = soup.find('li', attrs={'class': 'SegmentedControlItem js-SegmentedControlItem',
                                      'data-segment-id': 'TOEFL PBT'})
    pbtScore = float(pbtSec.find('div', attrs={'class': 'Score js-Score'}).string)
    langReq['TOFEL PBT'] = pbtScore
  except:
    pass
  record.append(langReq)
  
  try: # Requirements
    reqs = []
    reqSec = soup.find('section', attrs={'id': 'AcademicRequirements',
                                         'class': 'AcademicRequirementsInformation'})
    for req in reqSec.find_all('li'):
      reqs.append(req.get_text())
    record.append(reqs)
  except:
    record.append([])
  
  try: # living costs
    cost = {}
    costSec = soup.find('ul', attrs={'class': 'LivingCosts'}).find('div', attrs={'class': 'Amount'})
    costInfo = costSec.find_all('span')
    cost['lower'] = float(costInfo[0].string)
    cost['upper'] = float(costInfo[1].string)
    cost['currency'] = str(costInfo[2].string)
    cost['period'] = str(costInfo[3].string[1:]) # omit '/'month)
    record.append(cost)
  except:
    record.append({})

  outQ.put(record)

# get program details
#%%
# get pcodes
programDF = pd.read_hdf('export/top150.h5', 'program')
pcodes = programDF['pcode'].to_list()
workers = 50
listenQueue = Queue(workers)

#%%
for i in range(workers):
  threading._start_new_thread(getProgramDetails, (pcodes.pop(), listenQueue))
  time.sleep(random.random()/2)

# record: [pcode, about, outline[], language{}, requirements[], livingCost{}]
records = []
cnt = workers
while True:
  try:
    out = listenQueue.get(timeout=60) # long enough for a thread to finish
  except: # queue is empty <=> tasks were finished
    break
  if out is not None:
      records.append(out)

  if pcodes: # not empty
    time.sleep(random.random()/2)
    threading._start_new_thread(getProgramDetails, (pcodes.pop(), listenQueue))
    if cnt % workers == 0:
      print('got {} programs'.format(cnt))
    cnt += 1

#%%
# export results
detailDF = pd.DataFrame(records, columns=['pcode', 'About', 'Course Outline', 'Language Requirements', 'General Requirements', 'Living Costs'])
detailDF.set_index('pcode', inplace=True)
detailDF.to_hdf('export/top150.h5', 'details')


#%%
