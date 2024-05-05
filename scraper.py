from bs4 import BeautifulSoup as bs # type: ignore
import requests
import concurrent.futures
import mysql.connector # type: ignore
import time

# start_time = time.time()

def getCategories():

  print("getting categories")

  request = requests.get(baseUrl)

  soup = bs(request.content, "lxml")

  raw_categories = soup.findAll("a", attrs={"class": "itm", "role":"menuitem"})

  categories = []

  for cat in raw_categories:

    try:
      categoryDetails = {
        "name": cat.find("span").text,
        "href": cat["href"].replace(baseUrl, "") if baseUrl in cat["href"] else cat["href"]
      }
      categories.append(categoryDetails)
      print("category : ", categoryDetails)
    except:
      pass

  return categories

def getPageProducts(url, i):
  print(f"Getting page {i} ...")
    
  response = requests.get(url + "?page=" + str(i))
  soup = bs(response.content, "lxml")

  articles = soup.find_all("article", attrs={"class", "c-prd"})

  products = []

  for article in articles:

    core_div = article.find("a", attrs={"class", "core"})
    info_div = article.find("div", attrs={"class", "info"})

    product = {}

    try:
      product["name"] = info_div.find("h3", attrs={"class", "name"}).text

      print("product : ", product["name"])
    except:
      product["name"] = ""
    try:
      product["href"] = core_div["href"]
    except:
      product["href"] = ""
    try:
      product["price"] = info_div.find("div", attrs={"class", "prc"}).text
    except:
      product["price"] = ""
    try:
      product["category1"] = core_div["data-ga4-item_category"]
    except:
      product["category1"] = ""
    try:
      product["category2"] = core_div["data-ga4-item_category2"]
    except:
      product["category2"] = ""
    try:
      product["category3"] = core_div["data-ga4-item_category3"]
    except:
      product["category3"] = ""
    try:
      product["category4"] = core_div["data-ga4-item_category4"]
    except:
      product["category4"] = ""
    try:
      product["stars"] = info_div.find("div", attrs={"class", "stars _s"}).text
    except:
      product["stars"] = ""
    try:
      product["number_reviews"] = info_div.find("div", attrs={"class", "rev"}).text.split("(")[1][:-1]
    except:
      product["number_reviews"] = "0"
    try:
      product["brand"] = core_div["data-gtm-brand"]
    except:
      product["brand"] = ""
    try:
      product["img"] = core_div.find("img")["data-src"]
    except:
      product["img"] = ""
    try:
      product["is_discount"] = True if len(info_div.find("div", attrs={"class", "bdg _dsct _sm"}).text) != 0 else False
    except:
      product["is_discount"] = False
    try:
      product["old_price"] = info_div.find("div", attrs={"class", "old"}).text
    except:
      product["old_price"] = ""
    try:
      product["discount"] = info_div.find("div", attrs={"class", "bdg _dsct _sm"}).text
    except:
      product["discount"] = "0%"
    
    products.append(product)

  return products

def getCategoryProducts(url):

  print("getting first page for: ", url)

  response = requests.get(url)

  soup = bs(response.content, "lxml")
  
  n_pages = soup.find("a", attrs={"aria-label": "Dernière page"})["href"]
  n_pages = int(n_pages.split("=")[1].split("#")[0])

  print("n_pages : ", n_pages)

  products = []

  with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
    futures = [executor.submit(getPageProducts, url, i) for i in range(1, n_pages + 1)]
    for future in concurrent.futures.as_completed(futures):
      products.extend(future.result())
  
  return products

def scrapeAllCategories():
  
  print("scraping all categories")

  with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
    futures = [executor.submit(getCategoryProducts, baseUrl + category["href"]) for category in categories ]
    for future in concurrent.futures.as_completed(futures):
      allProducts.extend(future.result())


def saveToMysql(items):
  print("connecting to mysql ...")

  mydb = mysql.connector.connect(
  host="127.0.0.1",
  user="louay",
  password=""
  )

  mydb.cursor().execute("SET GLOBAL max_allowed_packet=268435456")

  print("Writing data ...")

  mydb.cursor().execute("CREATE DATABASE IF NOT EXISTS jumia_scraping")

  mydb.cursor().execute("USE jumia_scraping")

  mydb.cursor().execute("DROP TABLE IF EXISTS products")

  mydb.cursor().execute("CREATE TABLE products (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), href VARCHAR(255), price VARCHAR(255), category1 VARCHAR(255), category2 VARCHAR(255), category3 VARCHAR(255), category4 VARCHAR(255), stars VARCHAR(255), number_reviews VARCHAR(255), brand VARCHAR(255), img VARCHAR(255), is_discount BOOLEAN, old_price VARCHAR(255), discount VARCHAR(255))")

  mydb.cursor().executemany("INSERT INTO products (name, href, price, category1, category2, category3, category4, stars, number_reviews, brand, img, is_discount, old_price, discount) VALUES (%s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", [(item["name"], item["href"], item["price"], item["category1"], item["category2"],item["category3"], item["category4"], item["stars"], item["number_reviews"], item["brand"], item["img"], item["is_discount"], item["old_price"], item["discount"]) for item in items])

  mydb.commit()

  print("Writing done")


print("Starting Jumia Scraper...")
print("Made by Louay Ghanney")

baseUrl = "https://www.jumia.com.tn"

categories = getCategories()

allProducts = []

scrapeAllCategories()

saveToMysql(allProducts)


print("Program done")

# print("Execution time : ", time.time() - start_time)