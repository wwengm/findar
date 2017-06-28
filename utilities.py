import requests
import os
from bs4 import BeautifulSoup


def web_crawler(link):
    # Pretend browser
    header = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest"
    }
    r = requests.get(link, headers=header)
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
