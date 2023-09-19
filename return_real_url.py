# preparation 
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
import time
from newspaper import Article
import mysql.connector
import newspaper
import os
import MultipleWorker
import concurrent.futures
from bs4 import BeautifulSoup
current_path = os.path.dirname(os.path.realpath(__file__))
# Replace with your database connection details
db_config = {
    "host": "192.168.2.108",
    "user": "finance",
    "password": "finance",
    "database": "finance_news",
    "auth_plugin":'mysql_native_password',
}

class ReturnRealUrl:
    def __init__(self,links): 
        self.links = links
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-extensions")
        # this option is for running in the background
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-software-rasterizer")
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--ignore-certificate-errors')
        chrome_options.add_argument('--allow-running-insecure-content')
        chrome_options.add_argument("blink-settings=imagesEnabled=false")
        self.driver = webdriver.Chrome(options=chrome_options)
            
    def go_to_yahoo_return_real_url(self):
        time.sleep(1)
        # iterate through all the links
        for idx,link in enumerate(self.links):
            self.driver.get(link)
            time.sleep(1) 
            page_source = self.driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            # find continuous button 
            a_tag = soup.find("a", class_="link caas-button")
            if a_tag:
                self.links[idx] = a_tag.get("href")
                print("link from other website")
            else:
                print(f"Yahoo own link  {link}  Yahoo own news")

    def quit(self):
        self.driver.quit()

    def run(self):
        self.go_to_yahoo_return_real_url()
        self.quit()

def create_nums_scrapers(num):
    scrapers = []
    for i in range(num):
        scraper = ReturnRealUrl(start_date="7/1/2020",end_date="7/11/2020",region="Worldwide",query="Python programming")
        scrapers.append(scraper)
    return scrapers

# Function to insert a news article into the database
def insert_article(article_data):
    try:
        # Connect to the MySQL database
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # SQL query to insert the article into the table
        insert_query = """
        INSERT IGNORE INTO news_articles (real_link, uuid, title, content)
        VALUES (%s, %s, %s, %s)
        """

        # Execute the query with the article data
        cursor.execute(insert_query, article_data)
        connection.commit()

        print(f"title:  {article_data[1]} uuid: {article_data[0]} inserted successfully!")

    except mysql.connector.Error as err:
        print(f"Error: {err}")

    finally:
        # Close the database connection
        if connection.is_connected():
            cursor.close()
            connection.close

def get_links_insert_info(symbol):
    # Connect to the MySQL database
    connection = mysql.connector.connect(**db_config)
    table_name = f"{symbol}_news" 
    # SQL query to select all the article URLs
    select_query = f"""
    SELECT link, uuid, title  FROM {table_name}
    """
    cursor = connection.cursor()
    cursor.execute(select_query)
    # Fetch all the rows
    news_data = cursor.fetchall()

    connection.close()
    return news_data 

def get_symbols():
    # Connect to the MySQL database
    connection = mysql.connector.connect(**db_config)
    # SQL query to select all the article URLs
    select_query = f"""
    SELECT symbol FROM yahoo_symbol
    """
    cursor = connection.cursor()
    cursor.execute(select_query)
    # Fetch all the rows
    symbols = cursor.fetchall()
    # get rid of the tuple
    symbols = [symbol[0] for symbol in symbols]
    connection.close()
    return symbols

def return_url_article(symbol):
    # for symbol in symbols:
    print(f"symbol: {symbol}")
    news_data = get_links_insert_info(symbol)
    links = []
    for news in news_data:
        links.append(news[0])
    return_real_url = ReturnRealUrl(links)
    return_real_url.run()
    real_links = return_real_url.links

    # iterate through news_data and real_links
    for idx,news in enumerate(news_data):
        article = newspaper.Article(real_links[idx])
        # Download and parse the article
        # if article download fail, skip it
        try:
            article.download()
            article.download()
            article.parse()
            # Extract the article content
            article_text = article.text        
            news_data[idx] = (real_links[idx],news[1],news[2],article_text)
            insert_article(news_data[idx])
        except:
            print(f"{symbol} article download fail {real_links[idx]}")
            continue

if __name__ == "__main__":
    # calculate the time for scraping
    start_time = time.time()
    # get symbol from database
    symbols = get_symbols()

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        # Start the load operations and mark each future with its URL
        future_to_url = {executor.submit(return_url_article, symbol): symbol for symbol in symbols}
        for future in concurrent.futures.as_completed(future_to_url):
            symbol = future_to_url[future]
            try:
                data = future.result()
            except Exception as exc:
                print('%r generated an exception: %s' % (symbol, exc))
            else:
                # article is protected by website such as some paid news
                print('%r error' % (symbol))
    # worker = MultipleWorker.Multiworker()
    # worker.multithread_init(return_url_article, df_new=symbols, nprocess=24)
   
    print(f"--- {time.time() - start_time} seconds ---")
    
