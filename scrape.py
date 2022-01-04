import sys
import time
import pandas as pd

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support import expected_conditions as EC


def initialize_driver(page_load_timeout=300):
    """
    Initialize web driver
    Params:
        page_load_timeout:number, default=300
    returns:
        webdriver
    """

    print("\n====== Web Driver Initializer ======")
    print("Initializing web driver")

    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--no-sandbox")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(page_load_timeout)

    print("Web driver initialized")

    return driver


def fetch_web_page(driver, url):
    """
    Fetch Web Page
    params:
        driver: webdriver
        url: String
    returns:
        webdriver
    """

    print("\n====== Fetching Web Page ======")
    print("Web Page Link\n\t- {}".format(url))

    driver.get(url)
    element = WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.ID, "stockwiseprice"))
    )

    return driver


def fetch_company_list(driver):
    """
    Fetch Company List
    params:
        driver: webdriver
    returns:
        List[Dict] e.g. [{"symbol": "AHPC", "value": "360"}]
    """

    print("\nFetching Company List")

    url = "http://www.nepalstock.com/stockWisePrices"
    driver = fetch_web_page(driver=driver, url=url)

    soup = BeautifulSoup(driver.page_source, 'html.parser')
    company_list = soup.find("select", {"id": "stock-symbol"})
    companies = [{"symbol": option.string, "value": option.get("value")} for option in company_list.find_all("option")[1:]]

    return companies


def fetch_company_trading_data(driver, company, start_date, end_date, limit=100000):
    """
    Fetch Company Trading Data
    params:
        driver: webdriver
        company: Dict
            symbol: Str
            value: Integer
        start_date: Date (YYYY-MM-DD)
        end_date: Date (YYYY-MM-DD)
        limit: Integer
    returns:
        DataFrame
    """
    url = "http://www.nepalstock.com/main/stockwiseprices/index/1/?startDate={start_date}&endDate={end_date}&stock-symbol={company_id}&_limit={limit}".format(
        start_date=start_date,
        end_date=end_date,
        company_id=company["value"],
        limit=limit
    )

    driver = fetch_web_page(driver=driver, url=url)
    trading_data = driver.find_element(By.XPATH, "/html/body/div[5]/table/tbody/tr[3]/td").text

    if trading_data == "No Data Available!":
        message = "Data Not Found!\nCompany: {}\nPeriod: {} - {}\n".format(
            company["symbol"],
            start_date,
            end_date
        )
        print(message)

        return
    else:
        print("Parsing Table Data")

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        table = soup.find("table", {"class": "table table-condensed table-hover"})
        table_data = [[cell.text.replace('\r', '').replace('\n', '') for cell in row.find_all(["td"])] for row in table.find_all("tr")[1:-1]]
        df = pd.DataFrame(table_data)

        return df


def save_company_trading_data(df, company, start_date, end_date):
    """
    Save Company Trading Data
    params:
        df: DataFrame
        company: Dict
            symbol: Str
            value: Integer
        start_date: Date (YYYY-MM-DD)
        end_date: Date (YYYY-MM-DD)
    """

    header = df.iloc[0]
    df = df[1:]
    df.columns = header

    file_name = "{stock_symbol}_{start_date}_{end_date}.csv".format(
        stock_symbol=company["symbol"].replace("/", "-"),
        start_date=start_date,
        end_date=end_date
    )
    file_path = "data/{}".format(file_name)

    print("Saving data in file - {}".format(file_name))

    df.to_csv(file_path, index=False)


def main():
    """
    Main Function
    """
    print("Starting NEPSE web scraping")

    driver = initialize_driver()

    start_date = "2000-01-01"
    end_date = "2021-12-31"

    companies = fetch_company_list(driver=driver)

    for company in companies:
        df = fetch_company_trading_data(
            driver=driver,
            company=company,
            start_date=start_date,
            end_date=end_date
        )

        if df is None:
            print("Skipping the process for {}".format(company["symbol"]))
        else:
            save_company_trading_data(df=df, company=company, start_date=start_date, end_date=end_date)

            # To prevent NEPSE or ISP from blocking your IP due to frequent requests
            print("Sleeping for 15s to avoid DoS attack in NEPSE server")
            time.sleep(15)


    print("\n NEPSE web scraping completed")

    driver.close()
    sys.exit()


if __name__ == "__main__":
    main()
