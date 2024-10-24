import json
import os
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed

import jieba
import mysql.connector
import requests
from pyecharts import options as opts
from pyecharts.charts import Bar, WordCloud
from pyecharts.globals import ThemeType
from pyecharts.render import make_snapshot
from snapshot_selenium import snapshot
from tqdm import tqdm

db_config = {
    'host': '106.15.79.229',
    'port': 3306,
    'user': 'root',
    'password': '910920',
    'database': 'demo'
}


class Database:
    def __init__(self):
        self.conn = None

    def connect(self):
        try:
            self.conn = mysql.connector.connect(**db_config)
        except mysql.connector.Error as e:
            print(f'连接数据库失败：{e}')

    def clear_comments_table(self):
        cursor = self.conn.cursor()
        try:
            cursor.execute("TRUNCATE TABLE comments;")
            print('数据已清除!!!')
            self.conn.commit()
        except mysql.connector.Error as e:
            print(f'清空表数据失败：{e}')
        finally:
            cursor.close()

    def insert_comments(self, comments_data):
        if not comments_data:
            return

        cursor = self.conn.cursor()
        sql = """
            INSERT INTO comments (userid, creation_time, content, score, product_color, product_size, buy_count, location, mobile_version)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        try:
            cursor.executemany(sql, comments_data)
            self.conn.commit()
            print(f'成功插入 {len(comments_data)} 条评论')
        except mysql.connector.Error as e:
            print(f'批量插入数据失败：{e}')
            self.conn.rollback()
        finally:
            cursor.close()

    def fetch_column_data(self, column_name):
        cursor = self.conn.cursor()
        sql = f"SELECT {column_name} FROM comments"
        cursor.execute(sql)
        result = cursor.fetchall()
        cursor.close()
        return [item[0] for item in result if item[0]]  # 排除空值

    def close(self):
        if self.conn:
            self.conn.close()


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
    database = Database()
    database.connect()
    if database.conn is None:
        return

    database.clear_comments_table()

    max_pages = 100
    thread_count = 2
    all_comments = []

    with ThreadPoolExecutor(max_workers=thread_count) as executor:
        futures = {executor.submit(fetch_comments_page, product_id, page, headers): page for page in range(max_pages)}
        for future in tqdm(as_completed(futures), total=len(futures), desc="爬取评论"):
            page_comments = future.result()
            if page_comments:
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
                    for comment in page_comments
                ])

    print(f'共爬取到 {len(all_comments)} 条评论')

    batch_size = 200
    for i in range(0, len(all_comments), batch_size):
        database.insert_comments(all_comments[i:i + batch_size])

    database.close()


# 生成并保存柱状图
def generate_bar_chart(data, title, filename):
    # 统计词频
    counter = Counter(data)
    labels, values = zip(*counter.items())

    # 创建柱状图
    bar = Bar(init_opts=opts.InitOpts(bg_color="white", theme=ThemeType.LIGHT))
    bar.add_xaxis(list(labels))
    bar.add_yaxis(title, list(values),
                  itemstyle_opts=opts.ItemStyleOpts(color="auto"))
    if len(labels) > 10:  # 如果标签数量大于 10，则旋转 x 轴标签
        bar.set_global_opts(
            title_opts=opts.TitleOpts(title=title),
            visualmap_opts=opts.VisualMapOpts(is_show=False, max_=max(values)),
            xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(rotate=45))
        )
    else:
        bar.set_global_opts(
            title_opts=opts.TitleOpts(title=title),
            visualmap_opts=opts.VisualMapOpts(is_show=False, max_=max(values))
        )
    # 检查并创建data目录
    output_dir = "data"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 保存柱状图为图片
    filepath = os.path.join(output_dir, filename)
    temp_html_path = "temp.html"  # 临时 HTML 文件路径
    bar.render(temp_html_path)  # 先渲染为临时 HTML
    make_snapshot(snapshot, temp_html_path, filepath)  # 使用 snapshot 将 HTML 转换为图片

    # 删除临时 HTML 文件
    if os.path.exists(temp_html_path):
        os.remove(temp_html_path)
        print(f"已删除临时文件: {temp_html_path}")

    print(f"{filename} 柱状图已保存到: {filepath}")


def generate_word_cloud(texts, filename):
    # 将评论文本分词
    all_words = []
    for text in texts:
        all_words.extend(jieba.lcut(text))

    # 统计词频
    counter = Counter(all_words)
    words, counts = zip(*counter.most_common(1000))  # 只取前1000个词

    # 创建词云
    wordcloud = WordCloud()
    wordcloud.add("", [list(z) for z in zip(words, counts)], word_size_range=[20, 100])
    wordcloud.set_global_opts(title_opts=opts.TitleOpts(title="Comments WordCloud"))

    # 检查并创建 data 目录
    output_dir = "data"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 将词云渲染为 PNG 图片
    filepath = os.path.join(output_dir, filename)
    temp_html_path = "render.html"  # 临时 HTML 文件路径
    wordcloud.render(temp_html_path)  # 渲染为临时 HTML
    make_snapshot(snapshot, temp_html_path, filepath)  # 使用 snapshot 将 HTML 转换为图片

    # 删除临时 HTML 文件
    if os.path.exists(temp_html_path):
        os.remove(temp_html_path)
        print(f"已删除临时文件: {temp_html_path}")

    print(f"{filename} 词云已保存到: {filepath}")


if __name__ == '__main__':
    url = 'https://item.jd.com/100004972871.html'
    get_comments_multithread(url)

    db = Database()
    db.connect()
    if db.conn is None:
        exit()

    # 生成 product_color 的柱状图并保存为图片
    product_colors = db.fetch_column_data("product_color")
    generate_bar_chart(product_colors, "Product Colors", "product_colors_bar.png")

    # 生成 product_size 的柱状图并保存为图片
    product_size = db.fetch_column_data("product_size")
    generate_bar_chart(product_size, "Product Size ", "product_size_bar.png")

    # 生成 location 的柱状图并保存为图片
    location = db.fetch_column_data("location")
    generate_bar_chart(location, "Location ", "location_bar.png")

    # 生成 buy_count 的柱状图并保存为图片
    buy_count = db.fetch_column_data("buy_count")
    generate_bar_chart(buy_count, "Buy_Count ", "buy_count_bar.png")

    # 生成评论内容的词云，并保存为 PNG 格式
    comments = db.fetch_column_data("content")
    generate_word_cloud(comments, "comments_wordcloud.png")
    db.close()
