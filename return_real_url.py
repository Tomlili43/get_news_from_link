# preparation 
import threading
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
import time
from newspaper import Article
import mysql.connector
import newspaper
import os
from bs4 import BeautifulSoup
current_path = os.path.dirname(os.path.realpath(__file__))
# Replace with your database connection details
db_config = {
    "host": "localhost",
    "user": "root",
    "password": "root",
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
        # for idx,link in self.links:
        # for loop
        for idx,link in enumerate(self.links):
            self.driver.get(link)
            time.sleep(1) 
            page_source = self.driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            # find continuous button 
            a_tag = soup.find("a", class_="link caas-button")

            # Extract and print the URLs (href attributes) of all <a> tags
            if a_tag:
                self.links[idx] = a_tag.get("href")
                print("link from other website")
            else:
                print(f"Yahoo own link  {link}  Yahoo own news")

            # a_tag_xpath = '//*[@id="caas-art-72627c28-0252-331f-9fb1-d9a2ac885927"]/article/div/div/div/div/div/div[2]/div[4]/div/a'
            # time.sleep(5)
            # try:
            #     if self.driver.find_element_by_xpath(a_tag_xpath):
            #         a_tag = self.driver.find_element_by_xpath(a_tag_xpath)
            #         time.sleep(1)
            #         # get url from a tag
            #         url = a_tag.get_attribute("href")
            #         link = url
            #         # return "url"
            # except NoSuchElementException:
            #     print('a_tag_xpath is not found')
                # return link

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

    connection.close()
    return symbols

def multiple_thread_scrape(links):
    for link in links:
        scraper = ReturnRealUrl(link)
        scraper.run()
        print(f"{link} is done")


if __name__ == "__main__":
    # calculate the time for scraping
    start_time = time.time()
    # get symbol from database
    symbols = get_symbols()
    # iterate through symbols
    for symbol in symbols:
        print(f"symbol: {symbol[0]}")
        news_data = get_links_insert_info(symbol[0])
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
                continue
    print(f"--- {time.time() - start_time} seconds ---")
    
   
    # multiple threads == multiple scrapers == multiple chrome 
    
    # threads = []
    # for scraper in scrapers:
    #     thread = threading.Thread(target=multiple_thread_scrape,args=(scraper,queries))
    #     threads.append(thread)
    #     thread.start()
    # for thread in threads:
    #     thread.join()
