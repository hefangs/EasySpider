import os
import time
from concurrent.futures import ThreadPoolExecutor

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm


def get_response(url, headers=None, stream=False, retries=3, delay=2):
    """
    通用的网络请求函数，增加重试机制，用于获取网页或图片的响应。
    """
    for attempt in range(retries):
        try:
            res = requests.get(url, headers=headers, stream=stream)
            if res.status_code == 200:
                return res
            else:
                print(f"Failed to fetch {url}, status code: {res.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"An error occurred while fetching {url}: {e}")
            if attempt < retries - 1:
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)
    return None


def get_soup(url, headers):
    """
    获取网页的 BeautifulSoup 对象
    """
    res = get_response(url, headers=headers)
    if res:
        return BeautifulSoup(res.text, 'html.parser')
    return None


def extract_album_urls_from_page(soup):
    """
    从页面的 soup 对象中提取专辑的 URL 列表
    """
    all_urls = []
    card_columns = soup.find(class_='card-columns')
    if card_columns is None:
        print(f"Could not find element with class 'card-columns'")
        return all_urls

    lis = card_columns.find_all('div', class_='card')
    if not lis:
        print(f"No 'div' elements found within 'card-columns'")
        return all_urls

    for item in lis:
        item_url = item.find('a').get('href')
        if item_url:
            all_urls.append(item_url)
        else:
            print(f"Could not find href in {item}")

    return all_urls


def fetch_all_page_urls():
    """
    获取所有页面中的专辑 URL
    """
    all_urls = []
    base_url = 'https://www.girl-atlas.com/'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
    }

    for i in range(1, 2):  # 这里的 range 可以根据需要扩展
        page_url = f'{base_url}?p={i}'
        soup = get_soup(page_url, headers)
        if soup:
            urls_on_page = extract_album_urls_from_page(soup)
            all_urls.extend(urls_on_page)

    return all_urls


def extract_filename_from_url(url):
    """
    从 URL 中提取文件名
    """
    return url.split('/')[-1].replace('!lrg', '')


def download_image(url, folder_path):
    """
    下载单张图片并保存到指定文件夹
    """
    response = get_response(url, stream=True)
    if response:
        filename = extract_filename_from_url(url)  # 使用提取文件名的函数
        filepath = os.path.join(folder_path, filename)
        with open(filepath, 'wb') as file:
            for chunk in response.iter_content(1024):
                file.write(chunk)
        # print(f"Downloaded: {filepath}")
    else:
        print(f"Failed to download image from {url}")


def process_album(base_url, url):
    """
    处理单个专辑的图片下载
    """
    full_url = f'{base_url}{url}'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
    }

    res = get_response(full_url, headers=headers)
    if res:
        soup = BeautifulSoup(res.text, 'html.parser')

        # 提取专辑标题作为文件夹名
        header_title = soup.find('h1', class_='header-title')
        if header_title is None:
            print(f"Could not find element with class 'header-title' on {full_url}")
            return
        folder_name = header_title.get_text(strip=True)

        # 创建专辑文件夹
        album_folder_path = os.path.join('img2', folder_name)
        if not os.path.exists(album_folder_path):
            os.makedirs(album_folder_path)

        gallery = soup.find(class_='gallery')
        if gallery is None:
            print(f"Could not find element with class 'gallery' on {full_url}")
            return

        # 查找所有的 'a' 标签
        a_tags = gallery.find_all('a')
        if not a_tags:
            print(f"No 'a' tags found within 'gallery' on {full_url}")
            return

        for a_tag in a_tags:
            img_url = a_tag.get('data-src')
            if img_url:
                # print(f"Found image URL: {img_url}")
                # 判断 img_url 是相对路径还是绝对路径
                if img_url.startswith('http') or img_url.startswith('https'):
                    # 绝对路径，直接使用
                    full_img_url = img_url
                elif img_url.startswith('/'):
                    # 相对路径，需要拼接 base_url
                    full_img_url = f'{base_url}{img_url}'
                else:
                    # 其他情况
                    print(f"Unrecognized format for image URL: {img_url}")
                    continue  # 跳过不符合条件的 URL

                download_image(full_img_url, album_folder_path)

    else:
        print(f"Failed to fetch {full_url}")


def download_images_from_albums(urls):
    """
    并发下载多个专辑中的图片
    """
    base_url = 'https://www.girl-atlas.com/'

    # 创建 img2 文件夹（如果不存在）
    if not os.path.exists('img2'):
        os.makedirs('img2')

    # 使用多线程处理每个专辑
    with ThreadPoolExecutor(max_workers=8) as executor:
        list(tqdm(executor.map(lambda url: process_album(base_url, url), urls), total=len(urls)))

    # 顺序处理每个专辑
    # for url in tqdm(urls, total=len(urls)):  # 用 tqdm 包装循环以显示进度条
    #     process_album(base_url, url)


if __name__ == '__main__':
    # 获取所有专辑的 URL
    album_urls = fetch_all_page_urls()
    print(album_urls, len(album_urls))

    # 下载所有专辑中的图片
    download_images_from_albums(album_urls)
