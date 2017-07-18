import quandl
import time
from .google import *


def quandlPrice(tics=[], mkt='US', year=3,
                begdate='', enddate='', APIkey='siR2qmhGmgy8PsJooLnJ'):
    quandl.ApiConfig.api_key = APIkey
    if (enddate == ''):
        enddate = datetime.now()
        begdate = enddate - relativedelta(years=year)
    else:
        enddate = datetime.strptime(str(enddate), '%Y%m%d')
        begdate = datetime.strptime(str(begdate), '%Y%m%d')

    enddate = enddate.strftime("%Y-%m-%d")
    begdate = begdate.strftime("%Y-%m-%d")

    count = 0
    while True:
        try:
            count += 1
            if (mkt == 'US' or mkt == 'us'):
                print("Getting data from quandl:WIKI/PRICES")
                df = quandl_us(tics, begdate, enddate)
            elif(mkt == 'HK' or mkt == 'hk'):
                print("Getting data from quandl:HKEX")
                print(
                    "Warning: Simple returns are provided on this database", UserWarning)
                df = quandl.get(['HKEX/%s.3' % x for x in tics],
                                start_date=begdate, end_date=enddate)
                df.columns = [['%s.HK' % x for x in tics]]
                df = df.applymap(lambda x: x / 100)
            else:
                print('Market must be US or HK.')
                return
            break
        except Exception:
            if count >= 5:
                print('Network Err')
                return
            else:
                time.sleep(1 * count)
                continue
    print('quandlPrice successful')
    return df


def quandl_us(tics, begdate, enddate):
    data = []
    df = pd.DataFrame()
    tasks = [x * 50 for x in range(len(tics) / 50 + 1)] + [len(tics)]
    for i in range(1, len(tasks)):
        print('Getting chunk %s ' % i)
        d = quandl.get_table('WIKI/PRICES',
                             qopts={'columns': [
                                    'ticker', 'date', 'adj_close']},
                             ticker=tics[tasks[i - 1]: tasks[i]],
                             date={'gte': begdate,
                                   'lte': enddate},
                             paginate=True)
        gpt = d.groupby('ticker')
        for ticker, df in gpt:
            df = df.set_index('date').drop('ticker', axis=1)
            df.columns = [ticker]
            data.append(df)
    if data:
        df = pd.concat(data, axis=1)
    return df


if __name__ == '__main__':
    from .datareader import *
    tics1 = getCons('SP500').iloc[:, 0].values.tolist()
    tics2 = getCons('HSI').iloc[:, 0].values.tolist()

    df1 = quandlPrice(tics1, mkt='US')
    df2 = quandlPrice(tics2, mkt='HK')
