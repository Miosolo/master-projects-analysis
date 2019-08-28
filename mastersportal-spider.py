#%%
from bs4 import BeautifulSoup
import requests
import re
import pandas as pd

#%%
# collect top universities: QS, THE, US News and Shanghai Jiaotong TOP 150
ucodePattern = r'^https://www.mastersportal.com/universities/(\d+)/'
# QS TOP 150
QSHtml = requests.get('https://www.mastersportal.com/rankings/1/world-university-rankings-times-higher-education.html')
soup = BeautifulSoup(QSHtml.content)
univList = soup.find_all(name='tr', attrs={'itemprop': 'itemListElement', 'class': 'RankingRow'})
QSRanking = {}
for u in univList:
  udetails = u.find(attrs={'data-title': 'Universities'})
  try:
    ucode = re.findall(ucodePattern, udetails.a.attrs['href'])[0]

    ranking2019 = int(u.find(attrs={'data-title': '2019'}).span.string)
    ranking2018 = int(u.find(attrs={'data-title': '2018'}).span.string)
    ranking2017 = int(u.find(attrs={'data-title': '2017'}).span.string)

    if ranking2017 > 150 and ranking2018 > 150 and ranking2019 > 150:
      continue
  except:
    continue

  QSRanking[int(ucode)] = {
    'name': udetails.a.span.string,
    '2019': int(ranking2019),
    '2018': int(ranking2018),
    '2017': int(ranking2017)
  }

#%%
# collect info from mastersportal.com
# attributes to collect
schools = []
schoolids = []
countries = []
cities = []
programs = []
programids = []
degrees = []
durations = []
fees = []
summaries = []
disciplines = []

disciplineDict = {
  24: 'Computer Science & IT',
}

for discCode, disc in disciplineDict.items():
  for page in range(10000):
    if page % 10 == 0:
      print("page", page+1)
    # query and receive json
    respRaw = requests.get(url='https://search.prtl.co/2018-07-23/',
      params={'q': 'di-{}|en-100|lv-master|de-fulltime|tc-USD'.format(discCode), 'start': page*10})
    if respRaw.status_code != 200:
      break
    
    for prog in respRaw.json():\
      # not in TOP
      if prog['organisation_id'] not in QSRanking.keys():
          continue
      
      try: # get info
        school = prog['organisation']
        schoolID = prog['organisation_id']
        country = prog['venues'][0]['country']
        city = prog['venues'][0]['city']
        program = prog['title']
        programID = prog['id']
        degree = prog['degree']
        summary = prog['summary']

        if prog['tuition_fee']['unit'] != 'year':
          raise AssertionError
        else:
          fee = prog['tuition_fee']['value']

        if prog['fulltime_duration']['unit'] == 'year':
          duration = prog['fulltime_duration']['value'] * 12
        elif prog['fulltime_duration']['unit'] == 'month':
          duration = prog['fulltime_duration']['value']
        elif prog['fulltime_duration']['unit'] == 'day':
          duration = prog['fulltime_duration']['value']/30
        else:
          raise AssertionError

      except:
        continue

      schools.append(school)
      schoolids.append(schoolID)
      countries.append(country)
      cities.append(city)
      programs.append(program)
      programids.append(programID)
      degrees.append(degree)
      disciplines.append(disc)
      summaries.append(summary)
      durations.append(duration)
      fees.append(fee)

#%%
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
programsDF['QS2019'] = programsDF['School ID'].map(lambda id: QSRanking[id]['2019'])
programsDF = programsDF[['School', 'QS2019', 'Country', 'City', 'Program', 'Degree', 
  'Duration /month', 'Tuition fee /USD']]
programsDF.to_csv('CS.csv')
#%%
