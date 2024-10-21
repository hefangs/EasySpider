import mysql.connector
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm


def fetch_movies(page, cursor):
    url = 'https://movie.douban.com/top250?start=' + str(page * 25) + '&filter='
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
    }
    res = requests.get(url, headers=headers)
    soup = BeautifulSoup(res.text, 'html.parser')
    movies = soup.find_all('li')

    for movie in movies:
        # 获取 rank
        rank_tag = movie.find('em')
        rank = rank_tag.text.strip() if rank_tag else ""

        # 获取标题信息，包括主标题和其他标题
        title_section = movie.find('div', class_='hd')
        if title_section:
            title_tags = title_section.find_all('span', class_='title')
            main_title = title_tags[0].text.strip() if title_tags else ""
            other_titles = title_section.find('span', class_='other').text.strip() if title_section.find('span',
                                                                                                        'other') else ""
            title = f"{main_title} {other_titles}"
        else:
            title = ""

        # 获取导演和主演信息
        details_tag = movie.find('div', class_='bd')
        if details_tag:
            details_text = details_tag.find('p').text.strip()
            director = details_text.split("导演: ")[1].split("主演: ")[0].strip() if "导演: " in details_text else ""
            actor = f"导演: {director}"
        else:
            actor = ""

        # 获取年份、国家和类型信息 (info)
        if details_tag:
            info_lines = details_tag.find_all('p')[0].text.strip().split('\n')
            info = info_lines[-1].strip() if info_lines else ""
        else:
            info = ""

        # 获取评分
        rating_tag = movie.find('span', class_='rating_num')
        rating = rating_tag.text.strip() if rating_tag else ""

        # 获取评价人数
        rating_count_tag = movie.find('div', class_='star')
        rating_count = rating_count_tag.find_all('span')[-1].text.strip() if rating_count_tag else ""

        # 获取短评引用
        quote_tag = movie.find('p', class_='quote')
        quote = quote_tag.find('span', 'inq').text.strip() if quote_tag else ""

        # 仅在有数据的情况下插入数据库
        if rank and title and actor and info and rating and rating_count and quote:
            # 将数据插入到数据库
            insert_query = """
                INSERT INTO movies (ranks, title, actor, info, rating, rating_count, quote)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            try:
                cursor.execute(insert_query, (rank, title, actor, info, rating, rating_count, quote))
            except mysql.connector.Error as err:
                print(f"Error: {err}")


def main():
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

    # 清空表数据
    cursor.execute("TRUNCATE TABLE movies;")
    db_connection.commit()

    # 使用循环获取多页数据
    for i in tqdm(range(0, 10), desc="Fetching Movies"):
        fetch_movies(i, cursor)

    # 提交事务并关闭连接
    db_connection.commit()
    cursor.close()
    db_connection.close()


if __name__ == '__main__':
    main()
