from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from lxml import html
import pandas as pd
import time
from datetime import datetime
from app.database.connection import engine


def extract():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")

    driver = webdriver.Remote(
        command_executor='http://remote_chromedriver:4444/wd/hub',
        options=chrome_options
    )
    wait = WebDriverWait(driver, 20)
    driver.get("https://www.nepalstock.com/today-price")
    data = []
    header_is_written = False 
    columns = []
    while True:
        try:
            wait.until(EC.presence_of_element_located((By.XPATH, "//table/tbody/tr")))
            print("Table located")
            page_source = driver.page_source
            tree = html.fromstring(page_source)

            if not header_is_written:
                columns = tree.xpath('//table/thead/tr/th/text()')
                header_is_written = True
                print("Header extracted")

            rows = tree.xpath("//table/tbody/tr")
            for row in rows:
                # sector_url = row.xpath(".//td/a/@href")
                # sector = "N/A"
                # if sector_url:
                #     driver.get("https://www.nepalstock.com/" + sector_url[0])
                #     sec = wait.until(EC.presence_of_element_located((By.XPATH, "//div[@class='box company mb-3']")))
                    
                #     sector_data = html.fromstring(driver.page_source).xpath(
                #         "//div/div[2]/ul[1]/li/div/li[2]/strong/text()"
                #     )
                #     sector = sector_data[0].strip() if sector_data else "N/A"
                #     driver.back()

                row_data = []
                cells = row.xpath("./td")
                for cell in cells:
                    cell_text = cell.xpath("./a/text() | ./text() | ./span/text()")
                    row_data.append("".join(cell_text).strip())
                
                # Append sector data to the row
                # row_data.append(sector)
                data.append(row_data)

            try:
                next_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//a[contains(text(), 'Next')]")))
                next_button.click()
                print("Next button clicked")
                time.sleep(2) 
            except TimeoutException:
                print("No more pages")
                break

        except TimeoutException:
            print("Table not found; page load issue.")
            break
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            break

    print("Data extraction completed")


    # Fetch the data from the span text
    scraping_date = tree.xpath("//div[@class='table__asofdate']/span/text()")
    if scraping_date:
        date_text = scraping_date[0]
        date_str = date_text.replace("As of ", "").split(",")[0] + "," + date_text.split(',')[1]
        formatted_date = datetime.strptime(date_str, "%b %d, %Y").strftime("%Y-%m-%d")
        print(formatted_date)
    for row in data:
        row.append(formatted_date)
    data.append(row)

    columns.append('date') 
    df = pd.DataFrame(data, columns=columns)
    try:
        df.to_sql("staging_area", con=engine, if_exists='replace', index=False)
        print("Data loading successful.")
    except Exception as e:
        print(f"An error occurred while loading data: {e}")
    finally:
        engine.dispose()