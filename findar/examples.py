from findar import *


def getSP500():
    tics = getCons('SP500').iloc[:, 0].values.tolist()
    special = ['AMT', 'LMT', 'NBL', 'NWL']  # AMT not on google Finance
    for x in special:
        tics.remove(x)
    df1 = googlePrice(tics).Close  # constituents
    df2 = googlePrice(['NYSE%3A' + x for x in special[1:]]).Close
    df2.columns = special[1:]
    df3 = googlePrice(['INDEXSP:.INX']).Close  # index
    df3.columns = ['SP500']
    df = pd.concat([df2, df1], axis=1)
    df = df.reindex_axis(sorted(df.columns), axis=1)
    df = pd.concat([df, df3], axis=1)
    return df


def getHSI():
    count = 0
    while True:
        try:
            tics = getCons('HSI').iloc[:, 0].values.tolist()
            count += 1
            break
        except Exception:
            if count >= 5:
                print('Network Err')
                return
            else:
                time.sleep(1)
                continue

    tics2g = ['HKG:%0.4d' % int(x) for x in tics]
    df = googlePrice(tics2g + ['INDEXHANGSENG:HSI']
                     ).Close  # constituents + index
    df.columns = ['%s.HK' % x for x in tics] + ['HSI']
    return df


def getLIBOR2():
    count = 0
    while True:
        try:
            df = getLIBOR()
            count += 1
            break
        except Exception:
            if count >= 5:
                print('Network Err')
                break
            else:
                time.sleep(1)
                continue
    return df


def getHKETF():
    etfhk = getETF('HK')
    tics = etfhk.iloc[:, 0].values.tolist()
    tics2g = ['HKG:%0.4d' % int(x) for x in tics]  # change to google format
    df = googlePrice(tics2g).Close
    df.columns = ['%s.HK' % x[4:] for x in df.columns.values.tolist()]
    return df


def getUSETF():
    etfus = getETF('US')
    tics = etfus.iloc[:, 0].values.tolist()
    df = googlePrice(tics).Close
    return df


data_dir = '/home/data/test'


# df = getBoardLot()
# df.to_csv('%s/hk_board_lots.csv' % data_dir)

# df = getLIBOR2()
# df.to_csv('%s/LIBOR.csv' % data_dir)


# df = getSP500()
# df.to_csv('%s/SP500_3Y_Close.csv' % data_dir)

# df = getHSI()
# df.to_csv('%s/HSI_3Y_Close.csv' % data_dir)


# df = getHKETF()
# df.to_csv('%s/HKETF_3Y_Close.csv' % data_dir)

df = getUSETF()
df.to_csv('%s/USETF_3Y_Close.csv' % data_dir)


df = getETFinfo('HK')
df.to_csv('%s/HKETF_info.csv' % data_dir)


df = getETFinfo('US')
df.to_csv('%s/USETF_info.csv' % data_dir)
