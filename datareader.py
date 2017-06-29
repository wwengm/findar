from datetime import datetime, timedelta
import pandas as pd
import pandas_datareader.data as web
from .utilities import bs_table_extractor, etfcom_extractor, setup
from dateutil.relativedelta import relativedelta
import warnings
import time
from multiprocessing import Pool, Event

warnings.filterwarnings('ignore')


def getCons(index):
    print('Getting %s constituents' % index)
    if(index == 'HSI'):
        base = "http://www.hsi.com.hk/HSI-Net/HSI-Net?"
        loc = "cmd=navigation&pageId=en.indexes.hsisis.hsi.constituents"
        tables = bs_table_extractor(base + loc, 'greygeneraltxt')[1:5]
        df = pd.concat([pd.DataFrame(x[2:]) for x in tables]).iloc[:, 1:3]
        df.columns = ['Symbol', 'Name']
        df.Symbol = df.Symbol.apply(lambda x: str(x).zfill(5))
        df = df.sort_values('Symbol')
        df = df.reset_index(drop=True)

    elif(index == 'SP500'):
        df = pd.read_html(
            "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]
        df.columns = df.iloc[0, :]
        df = df.drop(0)
        df = df.drop('SEC filings', axis=1)

    elif(index == 'SP100'):
        df = pd.read_html(
            "https://en.wikipedia.org/wiki/S%26P_100")[2]
        df.columns = df.iloc[0, :]
        df = df.drop(0)

    else:
        print('Index Error')
        return
    print('getCons successful')
    return df


def getBoardLot():
    print('Connecting to hkex')
    adr = 'https://www.hkex.com.hk/eng/market/sec_tradinfo/stockcode/'
    security_type = {
        'Equity Securities': 'eisdeqty.htm',
        'Depositary Receipts (HDRs)': 'eisdhdr.htm',
        'Equity Warrants': 'eisdew.htm',
        'Investment Companies': 'eisdic.htm',
        'Exchange Traded Funds (ETFs)': 'eisdetf.htm',
        'Leveraged and Inverse Products (L&I Products)': 'liproducts.htm',
        'Real Estate Investment Trusts (REITs)': 'eisdreit.htm',
        'Other Unit Trusts/Mutual Funds': 'eisdtrus.htm',
        'Derivative Warrants (DWs)': 'eisdwarr.htm',
        'Callable Bull/Bear Contracts (CBBCs)': 'eisdcbbc.htm',
        'Trading Only Securities': 'eisdnadq.htm',
        'GEM Equity Securities': 'eisdgems.htm',
        'GEM Equity Warrants': 'eisdgemw.htm'
    }
    df_all = []
    for item in sorted(security_type):
        print('Getting %s' % item)
        link = adr + security_type[item]
        table = bs_table_extractor(link, 'table_grey_border')
        df = pd.DataFrame(table[0][1:]).iloc[:, :-4]
        df.columns = [x.replace('\r\n\t\t\t\t\t                 ', '')
                      for x in table[0][0][:-1]]
        df['BOARD LOT'] = df['BOARD LOT'].apply(
            lambda x: int(x.replace(',', '')))
        df_all.append(df)
    df = pd.concat(df_all)
    df = df.sort_values('STOCK CODE')
    df = df.reset_index(drop=True)
    print('getBoardLot successful')
    return df


def getLIBOR(year=3, begdate='', enddate='',
             data=['ONT', '1WK', '1MT', '3MT', '6MT', '12M'], currency=['USD']
             ):
    if (enddate == ''):
        enddate = datetime.now()
        begdate = enddate - relativedelta(years=year)
    else:
        enddate = datetime.strptime(str(enddate), '%Y%m%d')
        begdate = datetime.strptime(str(begdate), '%Y%m%d')
    for curr in currency:
        print('Getting %s LIBOR ' % curr)
        series_name = []
        for item in data:
            series_name.append("%s%sD156N" % (curr, item))
        df = web.DataReader(series_name, 'fred', begdate, enddate)
        df.columns = data

        # fill to most recent day
        newlist = []
        last_line = df.ix[-1].tolist()
        begin = df.index[-1].to_pydatetime().date()
        end = datetime.now().date()
        i = 1
        while True:
            new_timestamp = begin + timedelta(days=i)
            newlist.append([new_timestamp] + last_line)
            if(new_timestamp == end):
                break
            i += 1
        df1 = pd.DataFrame(newlist)
        df1 = df1.set_index(0)
        df1.columns = df.columns
        df = df.append(df1)
        df.index = pd.DatetimeIndex(df.index).normalize()
    print('getLIBOR successful')
    return df


def getETF(mkt=''):
    if (mkt == 'US' or mkt == 'us'):
        exchanges = {'Chicago Stock Exchange (CHX)': 'chx',
                     'NASDAQ OMX BX (BEX)': 'bex',
                     'NYSE Arca (ARCA)': 'arca',
                     'NYSE MKT (NYSE AMEX)': 'amex'}
        etf_link = 'https://www.interactivebrokers.com.hk/en/index.php?f=567&exch='
        etf_list = []
        for exch in sorted(exchanges):
            print('Getting ETF list of %s' % exch)
            link = etf_link + exchanges[exch]
            df_t = pd.read_html(link)[2]
            etf_list.append(df_t)

        full_list = pd.concat(etf_list).drop_duplicates().sort_values(
            'IB Symbol').reset_index(drop=True)
        columns = list(full_list.columns)
        columns[1] = 'Description'
        full_list.columns = columns

    elif (mkt == 'HK' or mkt == 'hk'):
        print('Getting ETF list from IB')
        adr_ib = 'https://www.interactivebrokers.com.hk/en/index.php?f=567&exch=sehk'
        df_ib = pd.read_html(adr_ib)[2]
        seclist_ib = df_ib['Symbol'].values.tolist()

        print('Getting ETF list from HKEX')
        adr_hkex = 'https://www.hkex.com.hk/eng/market/sec_tradinfo/stockcode/eisdetf.htm'
        a = bs_table_extractor(adr_hkex, 'table_grey_border')
        seclist_hkex = pd.DataFrame(a[0][1:])[0].values.tolist()

        hk_etf_list = []
        for sec in seclist_hkex:
            idsec = int(sec)
            if idsec in seclist_ib:
                hk_etf_list.append(idsec)

        full_list = df_ib[df_ib.Symbol.apply(lambda x: x in hk_etf_list)]
        columns = list(full_list.columns)
        columns[1] = 'Description'
        full_list.columns = columns
    else:
        print('Market must be US or HK')
        return
    print('getETF successful')
    return full_list


def getETFinfo(mkt='', processes=10):
    if (mkt == 'US' or mkt == 'us'):
        print('Getting ETF info from ETF.com')
        etf_list = getETF(mkt='US')
        tics = map(str, etf_list.Symbol.values.tolist())

        event = Event()
        p = Pool(processes, setup, (event,))
        rs = p.map_async(etfcom_extractor, tics, chunksize=1)
        event.set()
        while not rs.ready():
            time.sleep(60)
            event.clear()  # pause after five seconds
            print("PAUSED, {} left".format(rs._number_left))
            time.sleep(5)
            event.set()
            print("RESUMED")
        ac = rs.get()
        info = []
        for content in ac:
            if content:
                info.append(content)
        info = pd.DataFrame(info).drop('', axis=1)
        info = info.set_index('Ticker')
    elif (mkt == 'HK' or mkt == 'hk'):
        print('Getting ETF info from HKEX')
        link = 'https://www.hkex.com.hk/eng/etfrc/ETFTA/ETFTradingArrangement.htm'
        classname = 'table_grey_border'
        data = bs_table_extractor(link, classname)

        res = []
        for a in data:
            df = pd.DataFrame(a[1:], columns=a[0] + ['unimp'])
            df = df.iloc[:, 1:15]
            res.append(df)
            info = pd.concat(res)
    else:
        print('Market must be US or HK')
        return
    print('getETFinfo successful')
    return info


if __name__ == '__main__':
    df1 = getCons('SP500')
    df2 = getCons('SP100')
    df3 = getCons('HSI')
    boardlot = getBoardLot()
    Libor = getLIBOR(year=3)
    etfhk = getETF('HK')
    etfus = getETF('US')
    etf_info_hk = getETFinfo('HK')
    etf_info_US = getETFinfo('US')
