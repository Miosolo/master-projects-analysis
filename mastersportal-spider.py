# %%
from math import ceil
import threading
from queue import Queue
from bs4 import BeautifulSoup
import requests
import re
import pandas as pd

# Get the rankings
# %%
# collect top universities: QS, THE, US News and Shanghai Jiaotong TOP 100
ucodePattern = r'^https://www.mastersportal.com/universities/(\d+)/'
# years in record
rankingConfig = {
    'QS': [2018, 2019, 2020],
    'USNews': [2018, 2019],
    'SJ': [2016, 2017, 2018],
    'THE': [2017, 2018, 2019]
}
rankingDF = None

# at least a "TOP 100" record for all rankings
for rankingOrg, recordYears in rankingConfig.items():
  with open('tmp/{}-ranking.html'.format(rankingOrg)) as rankingHtml:
    soup = BeautifulSoup(rankingHtml)
    univList = soup.find_all(name='tr',
                             attrs={'itemprop': 'itemListElement', 'class': 'RankingRow'})

    rankingList = []
    for u in univList:
      udetails = u.find(attrs={'data-title': 'Universities'})

      # university code in DB
      try:
        ucode = re.findall(ucodePattern, udetails.a.attrs['href'])[0]
      except:
        continue

      # rankRecord = [code, name, ranks...]
      rankRecord = [ucode, re.sub(r'\s+', ' ', udetails.a.span.string)]
      rank = []
      try:  # some records may be empty
        for yr in recordYears:
          rank.append(int(u.find(attrs={'data-title': yr}).span.string))
      except:
        continue

      # at least one <100 record
      if min(rank) > 100:
        continue
      # add to tail
      rankRecord.extend(rank)
      rankingList.append(rankRecord)

    orgRankingDF = pd.DataFrame(rankingList,
                                columns=['ucode', 'Univ.'] + ['{}-{}'.format(rankingOrg, yr) for yr in recordYears])
    if rankingDF is None:
      rankingDF = orgRankingDF
    else:
      rankingDF = pd.merge(rankingDF, orgRankingDF, how='outer', on=[
                           'ucode', 'Univ.']).set_index('ucode')

# shrink to Int64
rankingDF.apply(lambda col: col.astype('Int64')
                if col.dtype == 'float64' else col)
rankingDF.to_csv('export/rankings.csv', index=False)

# Get the program records
# %%
# import csv ranking
rankingDF = pd.read_csv('export/rankings.csv').set_index('ucode').apply(lambda col: col.astype('Int64')
                                                                        if col.dtype == 'float64' else col)

# %%
def getPrograms(discCode, page, topUnivs, outQ):
  "func to get programs online in a thread"
  if page % 10 == 0:
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
    # program, pcode, degree, duration, fee, summary]
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
      print(prog)  # show in log
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
batchSize = 10
topUnivs = rankingDF['Univ.'].tolist()
listenQueue = Queue(batchSize)
programTable = []

for discCode in disciplineDict.keys():
  # get total numbers
  testReq = requests.get(url='https://search.prtl.co/2018-07-23/',
                         params={'q': 'di-{}|en-1|lv-master|de-fulltime|tc-USD'.format(discCode)})
  # totalPages = int(testReq.headers['total'])
  totalPages = 50

  for b in range(ceil(totalPages/batchSize)):
    for r in range(batchSize):
      # getPrograms(discCode, b*batchSize+r, topUnivs, listenQueue)
      threading._start_new_thread(
          getPrograms, (discCode, b*batchSize+r, topUnivs, listenQueue))
    for r in range(batchSize):
      out = listenQueue.get(timeout=5)
      if out is None:
        continue
      else:
        programTable.extend(out)  # 2d-table


# %%
programsDF = pd.DataFrame({
    'School': schools,
    'School ID': schoolids,
    'Country': countries,
    'City': cities,
    'Discipline': disciplines,
    'Program': programs,
    'Program ID': programids,
    'Degree': degrees,
    'Duration /month': durations,
    'Tuition fee /USD': fees,
    'Description': summaries,
})
programsDF['QS2019'] = programsDF['School ID'].map(
    lambda id: QSRanking[id]['2019'])
programsDF = programsDF[['School', 'QS2019', 'Country', 'City', 'Program', 'Degree',
                         'Duration /month', 'Tuition fee /USD']]
programsDF.to_csv('CS.csv')
# %%
