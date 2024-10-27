import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import mysql.connector
import requests
from dotenv import load_dotenv
from tqdm import tqdm

db_config = {
    'host': '106.15.79.229',
    'port': 3306,
    'user': 'root',
    'password': '910920',
    'database': 'demo'
}


def github_token():
    # 加载 .env 文件
    load_dotenv()
    # 获取 GitHub 令牌
    token = os.getenv('GITHUB_TOKEN')
    return token


class Database:
    def __init__(self):
        self.conn = None

    def connect(self):
        try:
            self.conn = mysql.connector.connect(**db_config)
        except mysql.connector.Error as e:
            print(f'连接数据库失败：{e}')

    def clear_table(self):
        cursor = self.conn.cursor()
        try:
            cursor.execute("TRUNCATE TABLE github_users;")
            print('数据已清除!!!')
            self.conn.commit()
        except mysql.connector.Error as e:
            print(f'清空表数据失败：{e}')
        finally:
            cursor.close()

    def insert_users(self, users_data):
        if not users_data:
            print("No data to insert.")
            return

        cursor = self.conn.cursor()
        sql = """
            INSERT INTO github_users (
                login, userid, username, followers, following, location, email,
                public_repos, public_gists
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
              """

        # 将数据分成每100条一组
        batch_size = 20
        total_inserted = 0
        try:
            for i in range(0, len(users_data), batch_size):
                batch = users_data[i:i + batch_size]
                cursor.executemany(sql, batch)
                self.conn.commit()
                total_inserted += len(batch)
                print(f"Successfully inserted {len(batch)} records.")

            print(f"Total records inserted: {total_inserted}")
        except mysql.connector.Error as e:
            print(f'批量插入数据失败：{e}')
            self.conn.rollback()
        finally:
            cursor.close()

    def close(self):
        if self.conn:
            self.conn.close()


def fetch_user_details(user_url, headers):
    """获取单个用户详细信息"""
    user_details_response = requests.get(user_url, headers=headers)
    if user_details_response.status_code == 200:
        user_details = user_details_response.json()
        return (
            user_details.get('login'),
            user_details.get('id'),
            user_details.get('name'),
            user_details.get('followers'),
            user_details.get('following'),
            user_details.get('location'),
            user_details.get('email'),
            user_details.get('public_repos'),
            user_details.get('public_gists')
        )
    return None


def fetch_all_user_details(user_urls, thread_count, headers, pbar):
    """使用多线程获取所有用户的详细信息"""
    with ThreadPoolExecutor(max_workers=thread_count) as executor:
        futures = {executor.submit(fetch_user_details, url, headers): url for url in user_urls}
        for future in as_completed(futures):
            user_data = future.result()
            if user_data:
                pbar.update(1)
                yield user_data


def get_top_followed_users(thread_count=2):
    token = github_token()
    params = {
        'q': 'repos:>0',  # 只搜索至少有一个仓库的用户
        'sort': 'followers',  # 按关注者数量排序
        'order': 'desc',  # 降序
        'per_page': 10,  # 每页返回10个结果
    }
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.183 Safari/537.36',
        'Authorization': f'token {token}'  # 使用格式化字符串插入令牌
    }

    url = "https://api.github.com/search/users"
    top_users = []  # 用于存储关注者最多的用户信息

    total_pages = 10
    total_users = total_pages * params['per_page']
    with tqdm(total=total_users, desc="Fetching user details") as pbar:
        for page in range(1, total_pages + 1):  # 获取前5页数据
            params['page'] = page
            response = requests.get(url, headers=headers, params=params)

            if response.status_code != 200:
                print(f"Failed to retrieve data on page {page}: {response.status_code}")
                return []

            users = response.json().get('items', [])  # 获取用户列表
            user_urls = [user.get('url') for user in users]  # 提取用户 URL 列表

            # 获取用户详细信息
            top_users.extend(fetch_all_user_details(user_urls, thread_count, headers, pbar))

    return top_users


if __name__ == '__main__':
    db = Database()
    db.connect()

    db.clear_table()
    all_user = get_top_followed_users(thread_count=10)

    db.insert_users(all_user)
    db.close()
