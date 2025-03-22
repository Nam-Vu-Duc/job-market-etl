from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from bs4 import BeautifulSoup
import requests
import pandas as pd
import random
import time
import os

chrome_options = Options()
chrome_options.add_argument("start-maximized")
chrome_options.add_argument("disable-blink-features=AutomationControlled")  # Hide Selenium

def get_job_list(driver) -> list[list[str]]:
    driver.execute_script("window.scrollBy(0, document.body.scrollHeight)")

    time.sleep(2)

    job_lists_container = WebDriverWait(driver, 20).until(
        ec.presence_of_element_located((By.CSS_SELECTOR, 'div.jobs-side-list'))
    )

    job_lists = job_lists_container.find_elements(By.CSS_SELECTOR, "div.figcaption")

    data = []
    for job in job_lists:
        try:
            position = job.find_element(By.CSS_SELECTOR, "h2 a").text.strip()
        except NoSuchElementException:
            position = "Not Available"

        try:
            company = job.find_element(By.CSS_SELECTOR, "a.company-name").text.strip()
        except NoSuchElementException:
            company = "Not Available"

        try:
            salary = job.find_element(By.CSS_SELECTOR, "div.salary p").text.strip()
        except NoSuchElementException:
            salary = "Not Available"

        try:
            address = job.find_element(By.CSS_SELECTOR, "div.location ul").text.strip()
        except NoSuchElementException:
            address = "Not Available"

        try:
            deadline = job.find_element(By.CSS_SELECTOR, "div.expire-date p").text.strip()
        except NoSuchElementException:
            deadline = "Not Available"

        data.append([position, company, salary, address, deadline])

    return data

def get_total_pages() -> int:
    driver = webdriver.Chrome(service=Service(), options=chrome_options)
    driver.get('https://careerviet.vn/viec-lam/data-engineer-k-trang-1-vi.html')

    try:
        pagination = WebDriverWait(driver, 10).until(
            ec.presence_of_element_located((By.CSS_SELECTOR, 'div.pagination ul'))
        )

    except (TimeoutException, NoSuchElementException):
        pagination = None

    if pagination.text:
        li_list = pagination.find_elements(By.CSS_SELECTOR, 'li')
        total_page = int(li_list[-2].text)
        return total_page

    driver.quit()

    return 1

def get_all_jobs() -> list[list[str]]:
    data = []
    total_page = get_total_pages()

    for i in range(1,total_page+1):
        driver = webdriver.Chrome(service=Service(), options=chrome_options)
        driver.get(f'https://careerviet.vn/viec-lam/data-engineer-k-trang-{i}-vi.html')
        data += get_job_list(driver)
        driver.quit()

    return data

# if __name__ == '__main__':
#     data = get_all_jobs()
#     data = pd.DataFrame(data)
#     data.columns = ['Vị trí', 'Công ty', 'Mức lương', 'Địa chỉ', 'Hạn nộp']
#     print(data)