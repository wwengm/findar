import requests
import os
import time
from bs4 import BeautifulSoup


def web_crawler(link):
    count = 0
    while True:
        try:
            # Pretend browser
            header = {
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
                "X-Requested-With": "XMLHttpRequest"
            }
            r = requests.get(link, headers=header)
            count += 1
            break
        except Exception:
            if count >= 5:
                print('Network Err')
                break
            else:
                time.sleep(1 * count)
                continue

    return (r.text)


def bs_table_extractor(link, classname):
    """
    return all tables from a website with the classname
    """
    text = web_crawler(link)
    soup = BeautifulSoup(text, "html.parser")
    tables = soup.find_all("table", {"class": classname})
    data = []
    for table in tables:
        temp = []
        for row in table.findAll("tr"):
            cols = row.find_all('td')
            cols = [ele.text.strip() for ele in cols]
            temp.append(cols)
        data.append(temp)
    return data


def makepath(path):
    try:
        os.makedirs(path)
        print('Folder Created: %s' % path)
    except Exception:
        print('Folder Exist: %s' % path)
        pass
    return


def setup(event):
    global unpaused
    unpaused = event


def etfcom_extractor(tic):
    link = 'http://www.etf.com/' + tic
    text = web_crawler(link)
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
                content['Ticker'] = tic
    else:
        print("%s Not found" % link)
    return content


def attempt(methodToRun, *args):
    count = 0
    while True:
        try:
            time.sleep(1 * count)
            count += 1
            result = methodToRun(*args)
            return result
        except Exception:
            if count >= 5:
                print('Maximum Retry Reached')
                return
            else:
                continue
    return
