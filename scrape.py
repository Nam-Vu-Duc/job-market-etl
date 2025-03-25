from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import mysql.connector
import time

chrome_options = Options()
chrome_options.add_argument("start-maximized")
chrome_options.add_argument("disable-blink-features=AutomationControlled")  # Hide Selenium

def get_job_from_top_cv(driver, conn, cur) -> None:
    job_lists_container = WebDriverWait(driver, 20).until(
        ec.presence_of_element_located((By.CLASS_NAME, 'job-list-search-result'))
    )

    job_lists = job_lists_container.find_elements(By.CLASS_NAME, 'job-item-search-result')

    for job in job_lists:
        try:
            position = job.find_element(By.CSS_SELECTOR, "h3.title a span").text.strip()
        except NoSuchElementException:
            position = "Not Available"

        try:
            company = job.find_element(By.CSS_SELECTOR, "a.company span").text.strip()
        except NoSuchElementException:
            company = "Not Available"

        try:
            salary = job.find_element(By.CSS_SELECTOR, "label.title-salary").text.strip()
        except NoSuchElementException:
            salary = "Not Available"

        try:
            address = job.find_element(By.CSS_SELECTOR, "label.address span").text.strip()
        except NoSuchElementException:
            address = "Not Available"

        try:
            exp = job.find_element(By.CSS_SELECTOR, "label.exp span").text.strip()
        except NoSuchElementException:
            exp = "Not Available"

        cur.execute(
            """
            INSERT INTO jobs.jobs(source, position, company, salary, address, exp) VALUES(%s, %s, %s, %s, %s, %s)
            """,
            ('topcv', position, company, salary, address, exp)
        )

        conn.commit()

def get_job_from_career_link(driver, conn, cur) -> None:
    job_lists_container = WebDriverWait(driver, 20).until(
        ec.presence_of_element_located((By.CSS_SELECTOR, 'ul.list-group.mt-4'))
    )

    job_lists = job_lists_container.find_elements(By.CSS_SELECTOR, "div.media-body.overflow-hidden")

    for job in job_lists:
        try:
            position = job.find_element(By.CSS_SELECTOR, "h5.job-name").text.strip()
        except NoSuchElementException:
            position = "Not Available"

        try:
            company = job.find_element(By.CSS_SELECTOR, "a.text-dark").text.strip()
        except NoSuchElementException:
            company = "Not Available"

        try:
            salary = job.find_element(By.CSS_SELECTOR, "span.job-salary").text.strip()
        except NoSuchElementException:
            salary = "Not Available"

        try:
            address = job.find_element(By.CSS_SELECTOR, "div.job-location div a").text.strip()
        except NoSuchElementException:
            address = "Not Available"

        cur.execute(
            """
            INSERT INTO jobs.jobs(position, company, salary, address) VALUES(%s, %s, %s, %s)
            """,
            (position, company, salary, address)
        )
        conn.commit()

def get_job_from_career_viet(driver, conn, cur) -> None:
    driver.execute_script("window.scrollBy(0, document.body.scrollHeight)")

    time.sleep(2)

    job_lists_container = WebDriverWait(driver, 20).until(
        ec.presence_of_element_located((By.CSS_SELECTOR, 'div.jobs-side-list'))
    )

    job_lists = job_lists_container.find_elements(By.CSS_SELECTOR, "div.figcaption")

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

def get_job_from_it_viec(driver, conn, cur) -> None:
    job_lists_container = WebDriverWait(driver, 20).until(
        ec.presence_of_element_located((By.CSS_SELECTOR, 'div.col-xl-5.card-jobs-list.ips-0.ipe-0.ipe-xl-6'))
    )

    job_lists = job_lists_container.find_elements(By.CSS_SELECTOR, "div.job-card")

    for job in job_lists:
        try:
            position = job.find_element(By.CSS_SELECTOR, "h3.imt-3").text.strip()
        except NoSuchElementException:
            position = "Not Available"

        try:
            company = job.find_element(By.CSS_SELECTOR, "span.ims-2.small-text.text-hover-underline a").text.strip()
        except NoSuchElementException:
            company = "Not Available"

        try:
            salary = job.find_element(By.CSS_SELECTOR, "div.d-flex.align-items-center.salary.text-rich-grey a").text.strip()
        except NoSuchElementException:
            salary = "Not Available"

        try:
            address = job.find_elements(By.CSS_SELECTOR, "span.ips-2.small-text.text-rich-grey")[1].text.strip()
        except NoSuchElementException:
            address = "Not Available"

def get_job_from_vietnam_work(driver) -> None:
    driver.execute_script("window.scrollBy(0, document.body.scrollHeight)")

    time.sleep(2)

    job_lists_container = WebDriverWait(driver, 20).until(
        ec.presence_of_element_located((By.CLASS_NAME, 'block-job-list'))
    )

    job_lists = job_lists_container.find_elements(By.CSS_SELECTOR, "div.sc-eEbqID.jZzXhN")

    for job in job_lists:
        try:
            position = job.find_element(By.CSS_SELECTOR, "h2 a").text.strip()
        except NoSuchElementException:
            position = "Not Available"

        try:
            company = job.find_element(By.CSS_SELECTOR, "div.sc-cdaca-d.dVvIA a").text.strip()
        except NoSuchElementException:
            company = "Not Available"

        try:
            salary = job.find_element(By.CSS_SELECTOR, "span.sc-fgSWkL.gKHoAZ").text.strip()
        except NoSuchElementException:
            salary = "Not Available"

        try:
            address = job.find_element(By.CSS_SELECTOR, "span.sc-kzkBiZ.hAkUGp").text.strip()
        except NoSuchElementException:
            address = "Not Available"

def get_all_jobs(conn, cur) -> None:
    for i in range(1, 21):
        driver = webdriver.Chrome(service=Service(), options=chrome_options)
        driver.get(f'https://www.topcv.vn/tim-viec-lam-cong-nghe-thong-tin-cr257?page={i}')
        get_job_from_top_cv(driver, conn, cur)
        driver.quit()

        # careerlink f'https://www.careerlink.vn/viec-lam/k/data-engineer?page={i}'
        # careerviet f'https://www.careerlink.vn/viec-lam/k/data-engineer?page={i}'
        # itviec f'https://www.careerlink.vn/viec-lam/k/data-engineer?page={i}'
        # vietnamwork f'https://www.careerlink.vn/viec-lam/k/data-engineer?page={i}'

def main():
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="root"
        )

        cur = conn.cursor()

        get_all_jobs(conn, cur)
    except Exception as e:
        print(e)

main()