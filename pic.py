import os
import random
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm


def fetch_all_page_urls():
    page_urls = []
    for i in range(0, 10):
        page_url = 'https://com.okmzt.net/photo/page/{}'.format(i)
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
        }

        res = requests.get(page_url, headers=headers)
        soup = BeautifulSoup(res.text, 'html.parser')

        lis = soup.find(class_='g-list').find_all('li')
        for item in lis:
            item_url = item.find('a').get('href')
            page_urls.append(item_url)
    print(page_urls, len(page_urls))
    return page_urls


def download_images(image_page_url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
    }
    res = requests.get(image_page_url, headers=headers)
    soup = BeautifulSoup(res.text, 'html.parser')

    # Set the directory to save images
    img_dir = "img"
    os.makedirs(img_dir, exist_ok=True)

    img_tags = soup.find_all('img', {'referrerpolicy': 'origin'})
    for img in img_tags:
        img_url = img.get('src')
        # print(img_url)

        # 确保 img_url 不是 None
        if img_url is not None:
            try:
                img_data = requests.get(img_url, headers=headers).content

                # 获取基础文件名和扩展名
                base_name, ext = os.path.splitext(img_url.split('/')[-1])

                # 生成一个 6 位随机数
                random_number = random.randint(100000, 999999)

                # 构建目标文件名，格式为 'base_name_randomNumber.ext'
                img_name = f"{base_name}_{random_number}{ext}"

                # 保存图片
                with open(os.path.join(img_dir, img_name), 'wb') as f:
                    f.write(img_data)

                return f"Downloaded {img_name}"
            except Exception as e:
                return f"Failed to download {img_url}: {str(e)}"
        else:
            return "Skipping image with no src attribute"


if __name__ == '__main__':
    urls = fetch_all_page_urls()

    # Set up thread pool
    max_threads = 4  # Adjust based on CPU and bandwidth
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        # Submit download tasks to the executor
        futures = [executor.submit(download_images, url) for url in urls]

        # Use tqdm to show the progress
        for future in tqdm(as_completed(futures), total=len(futures), desc="Downloading images", unit="page"):
            future.result()  # This will raise any exceptions caught during download
