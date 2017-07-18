from findar import *

data_dir = '/home/data/test'

df = getBoardLot()
df.to_csv('%s/hk_board_lots.csv' % data_dir)

df = attempt(getLIBOR)
df.to_csv('%s/LIBOR.csv' % data_dir)

df1 = getCons('SP500')  # sp 500 constituent list
df1.to_csv('%s/SP500_constituents.csv' % data_dir, encoding='utf-8')

df2 = quandlPrice(df1.iloc[:, 0].values.tolist(), mkt='US')
df2.to_csv('%s/SP500_close_price.csv' % data_dir)

df3 = googlePrice(['INDEXSP:.INX']).Close
df3.columns = ['SP500']
df3.to_csv('%s/SP500_index.csv' % data_dir)

# same as getCons('HSI'), but with multiple tries
df4 = attempt(getCons, 'HSI')
df4.to_csv('%s/HSI_constituents.csv' % data_dir)

df5 = quandlPrice(df4.iloc[:, 0].values.tolist(), mkt='HK')
df5.to_csv('%s/HSI_returns.csv' % data_dir)

df6 = googlePrice(['INDEXHANGSENG:HSI']).Close
df6.columns = ['HSI']
df6.to_csv('%s/HSI_index.csv' % data_dir)

df7 = getETF('HK')
df7.to_csv('%s/HK_ETF_list.csv' % data_dir)

df7list = df7.iloc[:, 0].values.tolist()
df7list = [str(x).zfill(5) for x in df7list]
df8 = quandlPrice(df7list, mkt='HK')
df8.to_csv('%s/HK_ETF_returns.csv' % data_dir)

df9 = getETF('US')
df9.to_csv('%s/US_ETF_list.csv' % data_dir)

df10 = googlePrice(df9.iloc[:, 0].values.tolist()).Close
df10.to_csv('%s/US_ETF_close_price.csv' % data_dir)

df11 = getETFinfo('HK')
df11.to_csv('%s/HKETF_info.csv' % data_dir)

df12 = getETFinfo('US')
df12.to_csv('%s/USETF_info.csv' % data_dir)
