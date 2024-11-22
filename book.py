import re
import chardet
import mysql.connector
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm


def fetch_books(page):
    """
    Fetches book data from a specific page of the website.
    """
    base_url = 'http://bang.dangdang.com/books/fivestars/01.00.00.00.00.00-all-0-0-1-{}'
    url = base_url.format(page)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
    }
    res = requests.get(url, headers=headers)
    encoding = chardet.detect(res.content)['encoding']
    res.encoding = encoding
    soup = BeautifulSoup(res.text, 'html.parser')
    books = soup.find_all('li')

    book_data = []
    for book in books:
        try:
            rank_tag = book.find('div', class_='list_num')
            rank = re.sub(r'\D', '', rank_tag.text.strip()) if rank_tag else ""

            title_tag = book.find('div', class_='name')
            title = title_tag.a['title'].strip(
            ) if title_tag and title_tag.a else ""

            comments_tag = book.find('div', class_='star')
            comments = comments_tag.a.text.strip() if comments_tag and comments_tag.a else ""

            publisher_info_tags = book.find_all('div', class_='publisher_info')
            author, publisher, publish_date = "", "", ""
            if len(publisher_info_tags) > 0:
                author = publisher_info_tags[0].text.strip()
            if len(publisher_info_tags) > 1:
                publish_date = publisher_info_tags[1].span.text.strip(
                ) if publisher_info_tags[1].span else ""
                publisher = publisher_info_tags[1].a.text.strip(
                ) if publisher_info_tags[1].a else ""

            rating_tag = book.find('div', class_='biaosheng')
            rating = rating_tag.span.text.strip() if rating_tag and rating_tag.span else ""

            price_tag = book.find('span', class_='price_n')
            price = price_tag.text.strip() if price_tag else ""

            # Append only if rank and title are available
            if rank and title:
                book_data.append((rank, title, comments or "", author or "",
                                 publisher or "", publish_date or "", rating or "", price or ""))
        except Exception as e:
            print(f"Error fetching book info: {e}")

    # print(f"Page {page}: {len(book_data)} books fetched.")
    return book_data


def batch_insert(cursor, book_data, batch_size=100):
    """
    Inserts book data into the database in batches.
    """
    insert_query = """
    INSERT INTO books (ranks, title, comments, author, publisher, publish_date, rating, price)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    total_inserted = 0
    for i in range(0, len(book_data), batch_size):
        batch = book_data[i:i + batch_size]
        try:
            cursor.executemany(insert_query, batch)
            inserted_count = len(batch)
            total_inserted += inserted_count
            print(f"Successfully inserted {inserted_count} records.")
        except mysql.connector.Error as err:
            print(f"Batch insert error: {err}")

    return total_inserted


def main():
    """
    Main function to scrape books and insert data into the database.
    """
    db_config = {
        'host': '106.15.79.229',
        'port': 3306,
        'user': 'root',
        'password': '910920',
        'database': 'demo'
    }

    # Connect to the database
    db_connection = mysql.connector.connect(**db_config)
    cursor = db_connection.cursor()

    # Clear the table
    cursor.execute("TRUNCATE TABLE books;")
    db_connection.commit()
    print('Data cleared!')

    all_book_data = []
    for i in tqdm(range(1, 26), desc="Fetching Books"):
        book_data = fetch_books(i)
        all_book_data.extend(book_data)

    # Remove duplicate entries
    # unique_books = list({tuple(book) for book in all_book_data})
    # unique_books = list(dict.fromkeys(all_book_data))
    # print(f"Total unique books fetched: {len(unique_books)}")

    # Insert data in batches
    total_inserted = batch_insert(cursor, all_book_data)
    print(f"Total records inserted into the database: {total_inserted}")

    # Commit and close the connection
    db_connection.commit()
    cursor.close()
    db_connection.close()


if __name__ == "__main__":
    main()
