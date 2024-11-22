import mysql.connector
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import time
import random
import logging

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_movies(page, retry_count=3):
    """
    Fetch movies from a specific page of Douban Top 250 with retry mechanism.
    """
    url = f'https://movie.douban.com/top250?start={page * 25}&filter='
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Cookie': 'bid=' + ''.join(random.choice('0123456789abcdef') for _ in range(11))
    }
    
    for attempt in range(retry_count):
        try:
            # 添加随机延时
            time.sleep(random.uniform(1, 3))
            
            res = requests.get(url, headers=headers, timeout=10)
            res.raise_for_status()
            
            if '豆瓣电影 Top 250' not in res.text:
                logging.warning(f"Page {page}: Possible anti-scraping response received")
                continue
                
            soup = BeautifulSoup(res.text, 'html.parser')
            movies = soup.find_all('div', class_='item')
            
            if not movies:
                logging.warning(f"Page {page}: No movies found in the response")
                continue
                
            movie_data = []
            for movie in movies:
                try:
                    rank_tag = movie.find('em')
                    rank = rank_tag.text.strip() if rank_tag else "N/A"

                    title_section = movie.find('div', class_='hd')
                    if not title_section:
                        continue
                        
                    title_tags = title_section.find_all('span', class_='title')
                    main_title = title_tags[0].text.strip() if title_tags else ""
                    other_title = title_section.find('span', class_='other')
                    other_titles = other_title.text.strip() if other_title else ""
                    title = f"{main_title} {other_titles}".strip()

                    details_tag = movie.find('div', class_='bd')
                    director = "N/A"
                    if details_tag and details_tag.find('p'):
                        details_text = details_tag.find('p').text.strip()
                        if "导演: " in details_text:
                            director = details_text.split("导演: ")[1].split("主演: ")[0].strip()
                    actor = f"导演: {director}"

                    info = "N/A"
                    if details_tag and details_tag.find_all('p'):
                        info_lines = details_tag.find_all('p')[0].text.strip().split('\n')
                        if len(info_lines) > 1:
                            info = info_lines[-1].strip()

                    rating_tag = movie.find('span', class_='rating_num')
                    rating = rating_tag.text.strip() if rating_tag else "N/A"

                    rating_count = "N/A"
                    rating_count_tag = movie.find('div', class_='star')
                    if rating_count_tag and rating_count_tag.find_all('span'):
                        count_text = rating_count_tag.find_all('span')[-1].text.strip()
                        rating_count = count_text

                    quote_tag = movie.find('p', class_='quote')
                    quote = quote_tag.find('span', 'inq').text.strip() if quote_tag and quote_tag.find('span', 'inq') else "N/A"

                    if title:
                        movie_data.append((rank, title, actor, info, rating, rating_count, quote))
                        
                except Exception as e:
                    logging.error(f"Error parsing movie: {str(e)}")
                    continue
                    
            if movie_data:
                logging.info(f"Successfully fetched {len(movie_data)} movies from page {page}")
                return movie_data
                
        except requests.RequestException as e:
            logging.error(f"Request failed on page {page}, attempt {attempt + 1}: {str(e)}")
            if attempt == retry_count - 1:
                logging.error(f"Failed to fetch page {page} after {retry_count} attempts")
                return []
            time.sleep(random.uniform(2, 5))
            
    return []

def batch_insert(cursor, movie_data, batch_size=50):
    """
    Inserts movie data into the database in batches.
    """
    insert_query = """
    INSERT INTO movies (ranks, title, actor, info, rating, rating_count, quote)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    total_inserted = 0
    
    # 计算需要插入的批次数
    num_batches = (len(movie_data) + batch_size - 1) // batch_size
    
    for i in (range(num_batches)):
        start_idx = i * batch_size
        end_idx = min((i + 1) * batch_size, len(movie_data))
        batch = movie_data[start_idx:end_idx]
        
        try:
            cursor.executemany(insert_query, batch)
            inserted_count = len(batch)
            total_inserted += inserted_count
            logging.info(f"Successfully inserted batch {i+1}/{num_batches} ({inserted_count} records)")
        except mysql.connector.Error as err:
            logging.error(f"Batch insert error: {err}")
            # 单条插入作为备选方案
            for record in batch:
                try:
                    cursor.execute(insert_query, record)
                    total_inserted += 1
                except mysql.connector.Error as err:
                    logging.error(f"Single record insert error: {err}")
                    continue
    
    return total_inserted

def main():
    """
    Main function with separated data collection and insertion.
    """
    try:
        # 数据库连接配置
        db_config = {
            'host': '106.15.79.229',
            'port': 3306,
            'user': 'root',
            'password': '910920',
            'database': 'demo',
            'connect_timeout': 30
        }

        # 先收集所有数据
        all_movie_data = []
        logging.info("Starting data collection...")
        
        # 使用循环获取多页数据
        for i in tqdm(range(10), desc="Fetching Movies"):
            movie_data = fetch_movies(i)
            if movie_data:
                all_movie_data.extend(movie_data)
                logging.info(f"Total movies collected so far: {len(all_movie_data)}")
        
        logging.info(f"Data collection completed. Total movies collected: {len(all_movie_data)}")
        
        # 如果没有收集到数据，提前退出
        if not all_movie_data:
            logging.error("No movie data collected. Exiting...")
            return

        # 连接数据库并插入数据
        logging.info("Connecting to database...")
        db_connection = mysql.connector.connect(**db_config)
        cursor = db_connection.cursor()

        # 清空表数据
        cursor.execute("TRUNCATE TABLE movies;")
        db_connection.commit()
        logging.info('Database cleared!')

        # 批量插入数据
        logging.info("Starting batch insertion...")
        total_inserted = batch_insert(cursor, all_movie_data)
        db_connection.commit()

        # 验证插入结果
        cursor.execute("SELECT COUNT(*) FROM movies")
        total_records = cursor.fetchone()[0]
        logging.info(f"Total records in database: {total_records}")

    except mysql.connector.Error as err:
        logging.error(f"Database error: {err}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'db_connection' in locals() and db_connection.is_connected():
            db_connection.close()
            logging.info("Database connection closed.")

if __name__ == '__main__':
    main()