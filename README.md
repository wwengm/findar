# findar
Financial Datareader

Financial Datareader provides data solutions to quantitative trading and backtesting.Currently provides up-to-date index constituent lists, full ETF lists trading boardlots, fundamental informations, Libor rates etc, for both US and HK market.

Example Usage:

In [29]: df = getBoardLot()
Connecting to hkex
Getting Callable Bull/Bear Contracts (CBBCs)
Getting Depositary Receipts (HDRs)
Getting Derivative Warrants (DWs)
Getting Equity Securities
Getting Equity Warrants
Getting Exchange Traded Funds (ETFs)
Getting GEM Equity Securities
Getting GEM Equity Warrants
Getting Investment Companies
Getting Leveraged and Inverse Products (L&I Products)
Getting Other Unit Trusts/Mutual Funds
Getting Real Estate Investment Trusts (REITs)
Getting Trading Only Securities
In [30]: df
Out[30]:
      BOARD LOT EXPIRY NAME OF LISTED SECURITIES NAME OF SECURITIES  \
0           500    NaN              CKH HOLDINGS                NaN
1           500    NaN              CLP HOLDINGS                NaN
2          1000    NaN            HK & CHINA GAS                NaN
3          1000    NaN            WHARF HOLDINGS                NaN
4           400    NaN             HSBC HOLDINGS                NaN
5           500    NaN              POWER ASSETS                NaN
6          2000    NaN              HOIFU ENERGY                NaN
7          1000    NaN                      PCCW                NaN
8          6000    NaN              NINE EXPRESS                NaN
9          1000    NaN           HANG LUNG GROUP                NaN
10          100    NaN            HANG SENG BANK                NaN
11         1000    NaN            HENDERSON LAND                NaN
12         1000    NaN                 HYSAN DEV                NaN
13         2000    NaN             VANTAGE INT'L                NaN
14         1000    NaN                   SHK PPT                NaN
15         1000    NaN             NEW WORLD DEV                NaN
16         2000    NaN            ORIENTAL PRESS                NaN
17          500    NaN           SWIRE PACIFIC A                NaN
18         1000    NaN                  WHEELOCK                NaN
19         5000    NaN             GREAT CHI PPT                NaN
20        40000    NaN                     MEXAN                NaN
21          200    NaN            BANK OF E ASIA                NaN
22         2000    NaN                   BURWILL                NaN
23         2000    NaN           CHEVALIER INT'L                NaN
