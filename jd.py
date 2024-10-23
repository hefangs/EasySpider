import json
from concurrent.futures import ThreadPoolExecutor, as_completed

import mysql.connector
import requests
from tqdm import tqdm

db_config = {
    'host': '106.15.79.229',
    'port': 3306,
    'user': 'root',
    'password': '910920',
    'database': 'demo'
}


def connect_db():
    try:
        db = mysql.connector.connect(**db_config)
        return db
    except mysql.connector.Error as e:
        print(f'连接数据库失败：{e}')
        return None


def clear_comments_table(db):
    cursor = db.cursor()
    try:
        cursor.execute("TRUNCATE TABLE comments;")
        print('数据已清除!!!')
        db.commit()
    except mysql.connector.Error as e:
        print(f'清空表数据失败：{e}')
    finally:
        cursor.close()


def insert_comments(db, comments):
    if not comments:
        return

    cursor = db.cursor()
    sql = """
        INSERT INTO comments (userid, creation_time, content, score, product_color, product_size, buy_count, location, mobile_version)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    try:
        cursor.executemany(sql, comments)
        db.commit()
        print(f'成功插入 {len(comments)} 条评论')
    except mysql.connector.Error as e:
        print(f'批量插入数据失败：{e}')
        db.rollback()
    finally:
        cursor.close()


def fetch_comments_page(product_id, page, headers):
    comment_url = f'https://club.jd.com/comment/productPageComments.action?callback=fetchJSON_comment98&productId={product_id}&score=0&sortType=5&page={page}&pageSize=10&isShadowSku=0&fold=1'
    try:
        response = requests.get(comment_url, headers=headers)
        response.raise_for_status()
        comment_json = response.text.replace('fetchJSON_comment98(', '').replace(');', '')
        comment_data = json.loads(comment_json)
        return comment_data['comments']
    except (requests.RequestException, json.JSONDecodeError) as e:
        print(f'页面 {page} 获取失败：{e}')
        return []


def get_comments_multithread(product_url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    product_id = product_url.split('/')[-1].split('.')[0]
    db = connect_db()
    if db is None:
        return

    clear_comments_table(db)

    max_pages = 100
    thread_count = 1
    all_comments = []

    with ThreadPoolExecutor(max_workers=thread_count) as executor:
        futures = {executor.submit(fetch_comments_page, product_id, page, headers): page for page in range(max_pages)}
        for future in tqdm(as_completed(futures), total=len(futures), desc="爬取评论"):
            comments = future.result()
            if comments:
                all_comments.extend([
                    (
                        comment.get('id'),
                        comment.get('creationTime'),
                        comment.get('content'),
                        comment.get('score'),
                        comment.get('productColor'),
                        comment.get('productSize'),
                        comment['extMap'].get('buyCount'),
                        comment.get('location'),
                        comment.get('mobileVersion'),
                    )
                    for comment in comments
                ])

    print(f'共爬取到 {len(all_comments)} 条评论')

    batch_size = 200
    for i in range(0, len(all_comments), batch_size):
        insert_comments(db, all_comments[i:i + batch_size])

    db.close()


if __name__ == '__main__':
    url = 'https://item.jd.com/100018517398.html'
    get_comments_multithread(url)
