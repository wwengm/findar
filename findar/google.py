from .utilities import web_crawler
from datetime import datetime
import pandas as pd
from bs4 import BeautifulSoup
from dateutil.relativedelta import relativedelta
import pandas_datareader.data as web
import warnings


class google(object):
    """
    Get daily stock prices from google
    For most US stocks, through API
    If failed, download through web_crawling
    """
    warnings.filterwarnings('ignore')
    base = 'https://www.google.com/finance/historical?'

    def __init__(self, tics=[], year=3, begdate='', enddate=''):
        self.data = {}
        self.tics = tics
        self.remain = tics
        if (enddate == ''):
            self.enddate = datetime.now()
            self.begdate = self.enddate - relativedelta(years=year)
            self.days = year * 365
        else:
            self.enddate = datetime.strptime(str(enddate), '%Y%m%d')
            self.begdate = datetime.strptime(str(begdate), '%Y%m%d')
            self.days = int(abs((self.enddate - self.begdate).days))

    def url_generator(self, symbol):
        n = int(self.days / 200) + 1
        url_list = []
        stock = 'q=%s' % symbol
        start = '&startdate=%s' % self.date_formatter(self.begdate)
        end = '&enddate=%s' % self.date_formatter(self.enddate)
        num = '&num=200'
        for i in range(n):
            start_row = '&start=%s' % (i * 200)
            url = self.base + stock + start + end + start_row + num
            url_list.append(url)
        return url_list

    def core(self):
        for symbol in self.remain:
            urls = self.url_generator(symbol)
            raw = []
            for url in urls:
                page = web_crawler(url)
                if ('Google automatically detects requests' in page):
                    raise Exception('GoogleTrafficBlock:Retry Later')

                soup = BeautifulSoup(page, "lxml")
                table = soup.select_one("table.gf-table.historical_price")
                if (table):
                    all_rows = table.find_all("tr")
                    raw += [x.text.split('\n') for x in all_rows[1:]]
                else:
                    break

            if (raw):
                try:
                    self.data[symbol] = self.result_formatter(raw)
                except:
                    print raw
                    print urls
        return

    def run(self):
        """
        Main Function: First obtain data from API
        If failed then obain by web crawling
        """
        try:
            print('Connecting Google API')
            wp = web.DataReader(self.tics, 'google',
                                self.begdate, self.enddate)
            exist = wp.minor_axis.values.tolist()
            for tic in exist:
                df = wp.minor_xs(tic)
                if not (df.dropna().empty):
                    df = df.applymap(lambda x: str(x).replace(',', ''))
                    self.data[tic] = df
            self.remain = [x for x in self.tics if x not in self.data]
            if (self.remain):
                print('%s stocks remains, searching Google_Web' %
                      len(self.remain))
        except Exception:
            pass
        self.core()
        self.remain = [x for x in self.tics if x not in self.data]
        wp = pd.Panel.from_dict(self.data, orient='minor')
        if (self.remain):
            print('%s not found on google Finance. Please check tickers' %
                  self.remain)
        return wp

    """
    Some Helper Functions
    """

    def date_formatter(self, datetime_object):
        month = datetime_object.strftime("%B")
        day = str(datetime_object.day).zfill(2)
        year = datetime_object.year
        fmt_date = '%s+%s%%2C+%s' % (month, day, year)
        return (fmt_date)

    def result_formatter(self, raw):
        df = pd.DataFrame(raw).drop_duplicates().iloc[:, 1:7]
        df.columns = ['Timestamp', 'Open', 'High',
                      'Low', 'Close', 'Volume']
        df.Timestamp = pd.to_datetime(df.Timestamp)
        df = df.sort_values('Timestamp')
        df = df.set_index('Timestamp')
        df = df.applymap(lambda x: str(x).replace(',', ''))
        return df


def googlePrice(tics=[], year=3, begdate='', enddate=''):
    a = google(tics, year, begdate, enddate)
    wp = a.run()
    return wp


if __name__ == '__main__':
    a = google(tics=['AAPL', 'HKG:0005', 'a_fake_ticker'], year=3)
    wpa = a.run()

    b = google(tics=['AAPL', 'HKG:0005', 'a_fake_ticker'],
               begdate=20160101, enddate=20170501)
    wpb = b.run()
