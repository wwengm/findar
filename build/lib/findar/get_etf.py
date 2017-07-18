import pandas as pd
from bs4 import BeautifulSoup
from multiprocessing import Pool
from utilities import web_crawler, bs_table_extractor
import time


def get_us_etf_list():
    """
    Grab full list of US ETFs from Interactive Brokers Website
    Returns:
        Pandas Dataframe
        columns = ['IB Symbol', 'Description', 'Symbol', 'Currency']
    """

    exchanges = {'Chicago Stock Exchange (CHX)': 'chx',
                 'NASDAQ OMX BX (BEX)': 'bex',
                 'NYSE Arca (ARCA)': 'arca',
                 'NYSE MKT (NYSE AMEX)': 'amex'}
    etf_link = 'https://www.interactivebrokers.com.hk/en/index.php?f=567&exch='
    etf_list = []
    for exch in sorted(exchanges):
        print('Obtaining ETF list of %s' % exch)
        link = etf_link + exchanges[exch]
        df_t = pd.read_html(link)[2]
        etf_list.append(df_t)

    full_list = pd.concat(etf_list).drop_duplicates().sort_values(
        'IB Symbol').reset_index(drop=True)
    columns = list(full_list.columns)
    columns[1] = 'Description'
    full_list.columns = columns
    return full_list


def get_HK_etf_list():
    """
    Grab intersection of HK ETF from Interactive Brokers Website and HKEX Website
    Returns:
        Pandas Dataframe
        columns = ['IB Symbol', 'Description', 'Symbol', 'Currency']
    """
    print('Obtaining ETF list from IB')
    adr_ib = 'https://www.interactivebrokers.com.hk/en/index.php?f=567&exch=sehk'
    df_ib = pd.read_html(adr_ib)[2]
    seclist_ib = df_ib['Symbol'].values.tolist()

    print('Obtaining ETF list from HKEX')
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

    return full_list


def fund_data_extractor(text):
    soup = BeautifulSoup(text, "html.parser")
    content = {}
    if not (soup.title.text == 'Sorry! | ETF.com'):
        a = soup.find_all("div", class_="breadcrumb")[0]
        content['Segment'] = str(a.text[19:])

        # general information
        a = soup.find_all("div", class_="generalData")
        content['Fund Description'] = a[0].find('p').text.encode('utf-8')
        # Other data
        for n in range(3, 6):
            c = a[n]
            rows = c.find_all("div", class_="rowText")
            for row in rows:
                field = str(row.find('label').text)
                desc = str(row.find('span').text)
                content[field] = desc
    else:
        print("Not found")
    return content


def func(tic):
    # print(tic)
    link = 'http://www.etf.com/' + tic
    text = web_crawler(link)
    content = fund_data_extractor(text)
    content['Ticker'] = tic
    return content


def get_us_etf_info(processes=16):
    """
    Grab ETF fundamental information from ETF.com
    """
    etf_list = get_us_etf_list()
    tics = map(str, etf_list.Symbol.values.tolist())

    p = Pool(processes=processes)
    rs = p.map_async(func, tics, chunksize=1)
    while not rs.ready():
        print("num left: {}".format(rs._number_left))
        time.sleep(5)
    ac = rs.get()
    info = []
    for content in ac:
        if content:
            info.append(content)
    info = pd.DataFrame(info).drop('', axis=1)
    info = info.set_index('Ticker')
    return info


def get_hk_etf_info():
    link = 'https://www.hkex.com.hk/eng/etfrc/ETFTA/ETFTradingArrangement.htm'
    classname = 'table_grey_border'
    data = bs_table_extractor(link, classname)

    res = []
    for a in data:
        df = pd.DataFrame(a[1:], columns=a[0] + ['unimp'])
        df = df.iloc[:, 1:15]
        res.append(df)
    return pd.concat(res)


def write_csv(df, path='.', filename='ETF_infomation'):
    df.to_csv('%s/%s.csv' % (path, filename))


if __name__ == '__main__':
    info = get_us_etf_info()
    write_csv(info, filename='US_ETF_infomation')
    info = get_hk_etf_info()
    write_csv(info, filename='HK_ETF_infomation')
