import chardet
import mysql.connector
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm


def get_all_books():
    # 远程数据库连接配置
    db_config = {
        'host': '106.15.79.229',
        'port': 3306,
        'user': 'root',
        'password': '910920',
        'database': 'demo'
    }

    # 连接到数据库
    db_connection = mysql.connector.connect(**db_config)
    cursor = db_connection.cursor()

    base_url = 'http://bang.dangdang.com/books/fivestars/01.00.00.00.00.00-all-0-0-1-{}'  # URL 模板
    total_pages = 20

    for page in tqdm(range(1, total_pages + 1), desc="Fetching books"):
        url = base_url.format(page)  # 更新URL
        res = requests.get(url)
        encoding = chardet.detect(res.content)['encoding']
        res.encoding = encoding  # 更新响应的编码
        soup = BeautifulSoup(res.text, 'html.parser')
        books = soup.find_all('li')
        for book in books:
            # 获取排名
            rank_tag = book.find('div', class_='list_num')
            rank = rank_tag.text.strip() if rank_tag else ""

            # 获取标题
            title_tag = book.find('div', class_='name')
            title = title_tag.a['title'].strip() if title_tag and title_tag.a else ""

            # 获取评论数量
            comments_tag = book.find('div', class_='star')
            comments = comments_tag.a.text.strip() if comments_tag and comments_tag.a else ""

            # 获取作者
            author_tag = book.find('div', class_='publisher_info')
            author = author_tag.a.text.strip() if author_tag and author_tag.a else ""

            # 获取出版社和出版日期
            publisher_tag = book.find_all('div', class_='publisher_info')
            publish_date = publisher_tag[1].span.text.strip() if len(publisher_tag) > 1 else ""
            publisher = publisher_tag[1].a.text.strip() if len(publisher_tag) > 1 and publisher_tag[1].a else ""

            # 获取评分次数
            rating_tag = book.find('div', class_='biaosheng')
            rating = rating_tag.span.text.strip() if rating_tag and rating_tag.span else ""

            # 获取价格
            price_tag = book.find('span', class_='price_n')
            price = price_tag.text.strip() if price_tag else ""

            # 仅在有数据的情况下插入数据库
            if rank and title and comments and author and publisher and rating and price:
                # 将数据插入到数据库
                insert_query = """
                INSERT INTO books (ranks, title, comments, author, publisher, publish_date, rating, price)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                try:
                    cursor.execute(insert_query,
                                   (rank, title, comments, author, publisher, publish_date, rating, price))
                except mysql.connector.Error as err:
                    print(f"Error: {err}")

    # 提交事务并关闭连接
    db_connection.commit()
    cursor.close()
    db_connection.close()


if __name__ == "__main__":
    get_all_books()
