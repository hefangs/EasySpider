import time

import requests


def get_page_urls():
    urls = []
    session = requests.Session()
    for i in range(1, 2):
        url = 'https://www.mzitu.com/page/{}'.format(i)
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Referer': 'https://com.okmzt.net/beauty/'
        }

        res = session.get(url, headers=headers)
        print(res.status_code)
        time.sleep(5)
    #     soup = BeautifulSoup(res.text, 'html.parser')
    #
    #     lis = soup.find(class_='g-list').find_all('li')
    #
    #     for item in lis:
    #         url = item.find('a').get('href')
    #         urls.append(url)
    # print(urls)
    # return urls


if __name__ == '__main__':
    get_page_urls()
