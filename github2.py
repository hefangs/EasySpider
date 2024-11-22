# -*- coding: utf-8 -*-
"""
Created on 2024/10/27 00:13
@author: he
@python_version: 3.13
Description: This is a description of the content.
"""
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
            print(f'Failed to connect to the database: {e}')

    def clear_table(self):
        cursor = self.conn.cursor()
        try:
            cursor.execute("TRUNCATE TABLE github_stars;")
            print('Table data cleared!')
            self.conn.commit()
        except mysql.connector.Error as e:
            print(f'Failed to clear table data: {e}')
        finally:
            cursor.close()

    def batch_insert(self, sql, data, batch_size=20):
        """Generic method to insert data in batches to avoid duplication."""
        cursor = self.conn.cursor()
        total_inserted = 0
        try:
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                cursor.executemany(sql, batch)
                self.conn.commit()
                total_inserted += len(batch)
                print(f"Inserted {len(batch)} records in batch.")

            print(f"Total records inserted: {total_inserted}")
        except mysql.connector.Error as e:
            print(f'Failed to insert data: {e}')
            self.conn.rollback()
        finally:
            cursor.close()

    def insert_users(self, users_data):
        """Inserts user data using the generic batch insert method."""
        if not users_data:
            print("No data to insert.")
            return

        sql = """
            INSERT INTO github_stars (
                userid, name, stars, forks, language, html_url 
            ) VALUES (%s, %s, %s, %s, %s, %s)
        """
        # Call the generic batch insert
        self.batch_insert(sql, users_data)

    def close(self):
        if self.conn:
            self.conn.close()


def fetch_repo_data(url, headers, params):
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        total_repos = response.json().get('items', [])
        repo_datas = [
            (repo['id'], repo['name'], repo['stargazers_count'],
            repo['forks_count'], repo['language'], repo['html_url'])
            for repo in total_repos
        ]
        return repo_datas
    else:
        print(f"Failed to fetch data with status code {response.status_code}")
        return []


def get_top_repos(thread_count=5, total_pages=5):
    token = github_token()
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
        'Authorization': f'token {token}'
    }
    base_url = "https://api.github.com/search/repositories"
    params = {
        'q': 'stars:>1',
        'sort': 'stars',
        'order': 'desc',
        'per_page': 10
    }

    all_repos = []
    # Set up thread pool and progress bar for each page fetch
    with ThreadPoolExecutor(max_workers=thread_count) as executor, tqdm(total=total_pages,
                                                                        desc="Fetching pages") as pbar:
        futures = [
            executor.submit(fetch_repo_data, base_url, headers, {**params, 'page': page})
            for page in range(1, total_pages + 1)
        ]
        for future in as_completed(futures):
            page_repos = future.result()
            if page_repos:
                all_repos.extend(page_repos)
            pbar.update(1)
    return all_repos


if __name__ == '__main__':
    # Connect to the database and insert data
    db = Database()
    db.connect()

    # Clear existing data
    db.clear_table()

    # Fetch top-starred repositories data with multithreading and progress bar
    repo_data = get_top_repos(thread_count=5, total_pages=10)

    # Insert fetched data into the database in batches
    db.insert_users(repo_data)

    # Close the database connection
    db.close()
