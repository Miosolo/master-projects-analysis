#%%
from bs4 import BeautifulSoup
import requests
import re
import pandas as pd

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

#%%
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

#%%
# shrink to Int64
rankingDF.apply(lambda col: col.astype('Int64')
                if col.dtype == 'float64' else col)
with pd.HDFStore('export/top150.h5', 'w') as f:
  f['ranking'] = rankingDF
rankingDF.to_csv('export/rankings.csv', index=False)
