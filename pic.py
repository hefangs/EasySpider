import os

import requests
from bs4 import BeautifulSoup


def get_page_urls():
    urls = []
    for i in range(0, 1):  # 这里可以修改范围以获取更多页面
        url = 'https://com.okmzt.net/photo/{}'.format(i)
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
        }

        res = requests.get(url, headers=headers)
        soup = BeautifulSoup(res.text, 'html.parser')

        lis = soup.find(class_='g-list').find_all('li')

        for item in lis:
            url = item.find('a').get('href')
            urls.append(url)
    print(urls)
    return urls


def download_images(urls):
    for url in urls:
        res = requests.get(url)
        soup = BeautifulSoup(res.text, 'html.parser')

        # 假设图片在页面中使用 <img> 标签，且需要的图片地址在 'src' 属性中
        img_tag = soup.find('img')
        if img_tag and 'src' in img_tag.attrs:
            img_url = img_tag['src']  # 获取图片 URL

            # 下载图片
            img_res = requests.get(img_url)
            if img_res.status_code == 200:
                # 从 URL 中提取文件名
                img_name = os.path.basename(img_url)
                with open(img_name, 'wb') as img_file:
                    img_file.write(img_res.content)
                print(f"下载成功: {img_name}")
            else:
                print(f"图片下载失败: {img_url}")
        else:
            print(f"未找到图片: {url}")


if __name__ == '__main__':
    urls = get_page_urls()  # 获取页面链接
    download_images(urls)  # 下载图片
