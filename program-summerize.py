# %%
import pandas as pd

# %%
# import csv ranking
rankingDF = pd.read_hdf('export/top150.h5', key='ranking').apply(lambda col: col.astype('Int64')
                                                                 if col.dtype == 'float64' else col)
rankingConfig = {
    'QS': [2018, 2019, 2020],
    'USNews': [2018, 2019],
    'SJ': [2016, 2017, 2018],
    'THE': [2017, 2018, 2019]
}
programDF = pd.read_hdf('export/top150.h5', key='data')

# %%
programDFExtended = pd.merge(
    programDF, rankingDF, left_on='ucode', right_index=True)

for org in rankingConfig.keys():
  programDFExtended[org] = programDFExtended[[
      '{}-{}'.format(org, yr) for yr in rankingConfig[org]]].mean(axis=1).round().astype('Int64')
  programDFExtended.drop(['{}-{}'.format(org, yr)
                          for yr in rankingConfig[org]], inplace=True, axis=1)

programDFExtended.drop(['ucode', 'pcode', 'Univ.'], axis=1, inplace=True)

cols = programDFExtended.columns.to_list()
programDFExport = programDFExtended[
    cols[0:3] + cols[-4:] + [cols[-5]] + cols[3:8]
]

# %%
programDFExport.sort_values(by=['QS', 'University'], inplace=True)
programDFExport.to_csv('export/top150-programs.csv', index=False)
