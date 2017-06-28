# coding: utf-8
import re
import sys
import urllib
import datetime
import pandas as pd
import pandas_datareader.data as web

from bs4 import BeautifulSoup


def getCons(index):
    result = []

    if(index == 'HSI'):
        base = "http://www.hsi.com.hk/HSI-Net/HSI-Net?"
        loc = "cmd=navigation&pageId=en.indexes.hsisis.hsi.constituents"
        htmlfile = urllib.urlopen(base + loc)
        htmltxt = htmlfile.read()
        pattern = re.compile(b'<div id="rt-const-name-(.+?)">(.+?)</div>')
        sec = re.findall(pattern, htmltxt)
        for item in sec:
            tic = 'HKG:%0.4d' % int(item[0])
            result.append(tic)

    elif(index == 'SP500'):
        htmlfile = urllib.urlopen(
            "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
        htmltxt = htmlfile.read()
        ptn1 = b'rel="nofollow" class="external text" '
        ptn2 = b'href=(.+?)>(.+?)</a></td>\n<td><a href=(.+?)'
        ptn3 = b' title="(.+?)">(.+?)'
        pattern1 = re.compile(ptn1 + ptn2 + ptn3)
        symbol = re.findall(pattern1, htmltxt)
        for i in range(len(symbol)):
            tic = symbol[i][1]
            result.append(tic)

    elif(index == 'SP100'):
        htmlfile = urllib.urlopen(
            "https://en.wikipedia.org/wiki/S%26P_100")
        htmltxt = htmlfile.read()
        ptn1 = b'\n<td>(.+?)</td>\n<td>'
        ptn2 = b'<a href="(.+?)" title="(.+?)">(.+?)</a></td>'
        pattern1 = re.compile(ptn1 + ptn2)
        symbol = re.findall(pattern1, htmltxt)
        for i in range(len(symbol)):
            tic = symbol[i][0]
            result.append(tic)
    else:
        print('Index Error')
        sys.exit()

    return result


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
        link = adr + security_type[item]
        print('Getting %s' % item)
        df_all.append(boardlot_formatter(link))
    df = pd.concat(df_all)[['STOCK CODE', 'NAME OF SECURITIES',
                            'BOARD LOT', 'EXPIRY',
                            'UNIT TRUSTS/FUND MANAGER', 'REMARK']]
    df = df.sort_values('STOCK CODE')
    df['BOARD LOT'] = df['BOARD LOT'].apply(lambda x: int(x.replace(',', '')))
    return df


def settime(year):
    enddate = datetime.datetime.now()
    y = enddate.year - year
    m = enddate.month
    d = enddate.day

    # adjust for Feb
    if(m == 2):
        if (y % 4 and d > 28):
            d = 28
        if (not(y % 4) and d > 29):
            d = 29

    begdate = datetime.datetime(y, m, d)
    return [begdate, enddate]


def getLIBOR(year=3,
             data=['ONT', '1WK', '1MT', '3MT', '6MT', '12M'], currency=['USD']
             ):

    print('Getting LIBOR ')
    start, end = settime(year)
    for curr in currency:
        series_name = []
        for item in data:
            series_name.append("%s%sD156N" % (curr, item))
        df = web.DataReader(series_name, 'fred', start, end)
        df.columns = data

        # fill to most recent day
        newlist = []
        last_line = df.ix[-1].tolist()
        begin = df.index[-1].to_pydatetime().date()
        end = datetime.datetime.now().date()
        i = 1
        while True:
            new_timestamp = begin + datetime.timedelta(days=i)
            newlist.append([new_timestamp] + last_line)
            if(new_timestamp == end):
                break
            i += 1
        df1 = pd.DataFrame(newlist)
        df1 = df1.set_index(0)
        df1.columns = df.columns
        df = df.append(df1)
        df.index = pd.DatetimeIndex(df.index).normalize()
    return df


def boardlot_formatter(link):
    page = urllib.urlopen(link)
    soup = BeautifulSoup(page, "html.parser")
    table = soup.find('table', class_='table_grey_border')
    raw = []
    header = []
    for row in table.findAll("tr"):
        cells = row.findAll('td')
        temp = []
        for item in cells:
            temp.append(item.find(text=True))
        temp = [x.encode('UTF8') for x in temp]
        if (not header):
            for i in range(len(temp)):
                temp[i] = temp[i].replace(
                    '\r\n\t\t\t\t\t                  ', ' ').lstrip().rstrip()
                if ('BOARD' in temp[i] and 'LOT' not in temp[i]):
                    temp[i] = 'BOARD LOT'
                if ('NAME OF LISTED SECURITIES'in temp[i]):
                    temp[i] = 'NAME OF SECURITIES'

            header = temp
        else:
            temp[0] = temp[0] + '.HK'
            remarkTemp = ''
            for i in range(len(header) - 1, len(temp)):
                if (temp[i] != '\xc2\xa0'):
                    remarkTemp += temp[i]
            temp = temp[:len(header) - 1] + [remarkTemp]
            raw.append(temp)

    df = pd.DataFrame(raw, columns=header)
    return df


if __name__ == '__main__':
    getCons('SP500')
    getCons('SP100')
    getCons('HSI')
    boardlot = getBoardLot()
    Libor = getLIBOR(year=3)
