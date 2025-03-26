from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from datetime import datetime
from confluent_kafka import SerializingProducer
import mysql.connector
import time
import math
import json

chrome_options = Options()
chrome_options.add_argument("start-maximized")
chrome_options.add_argument("disable-blink-features=AutomationControlled")  # Hide Selenium

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {msg}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def insert_to_mysql(conn, cur, data) -> None:
    cur.execute(
        """
        INSERT INTO jobs.jobs(source, position, company, salary, address, exp, query_day) VALUES(%s, %s, %s, %s, %s, %s, %s)
        """,
        (data['source'], data['position'], data['company'], data['salary'], data['address'], data['exp'], datetime.today().strftime('%Y-%m-%d'))
    )
    conn.commit()
    return

def insert_to_kafka(producer, data) -> None:
    producer.produce(
        'jobs-topic',
        key=data['position'],
        value=json.dumps(data),
        on_delivery=delivery_report
    )

def get_job_from_top_cv(conn, cur, producer) -> None:
    # get total pages
    driver = webdriver.Chrome(service=Service(), options=chrome_options)
    driver.get('https://www.topcv.vn/tim-viec-lam-cong-nghe-thong-tin-cr257')

    pagination = WebDriverWait(driver, 20).until(
        ec.presence_of_element_located((By.ID, 'job-listing-paginate-text'))
    )
    total_page = int(pagination.text.split()[2])

    driver.quit()

    for i in range(1, total_page+1):
        # open web
        driver = webdriver.Chrome(service=Service(), options=chrome_options)
        driver.get(f'https://www.topcv.vn/tim-viec-lam-cong-nghe-thong-tin-cr257?page={i}')

        # get job elements, use presence_of_element_located to await til the element appears
        job_lists_container = WebDriverWait(driver, 20).until(
            ec.presence_of_element_located((By.CLASS_NAME, 'job-list-search-result'))
        )
        job_lists = job_lists_container.find_elements(By.CLASS_NAME, 'job-item-search-result')

        # query each job in each page
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

            data = {
                'source'    : 'topcv',
                'position'  : position,
                'company'   : company,
                'salary'    : salary,
                'address'   : address,
                'exp'       : exp
            }

            insert_to_mysql(conn, cur, data)
            insert_to_kafka(producer, data)

        driver.quit()

def get_job_from_career_link(conn, cur, producer) -> None:
    # get total pages
    driver = webdriver.Chrome(service=Service(), options=chrome_options)
    driver.get('https://www.careerlink.vn/vieclam/tim-kiem-viec-lam?category_ids=130%2C19&page=1')
    try:
        pagination = WebDriverWait(driver, 10).until(
            ec.presence_of_element_located((By.CSS_SELECTOR, 'ul.pagination'))
        )
    except (TimeoutException, NoSuchElementException):
        pagination = None

    if pagination:
        li_list = pagination.find_elements(By.CSS_SELECTOR, 'li')
        total_page = int(li_list[-2].text)
    else:
        total_page = 1

    # get jobs each page
    for i in range(1, total_page+1):
        # open web
        driver.get(f'https://www.careerlink.vn/vieclam/tim-kiem-viec-lam?category_ids=130%2C19&page={i}')

        # get job elements, use presence_of_element_located to await til the element appears
        job_lists_container = WebDriverWait(driver, 20).until(
            ec.presence_of_element_located((By.CSS_SELECTOR, 'ul.list-group.mt-4'))
        )
        job_lists = job_lists_container.find_elements(By.CSS_SELECTOR, "div.media-body.overflow-hidden")

        # query each job in each page
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

            data = {
                'source'    : 'careerlink',
                'position'  : position,
                'company'   : company,
                'salary'    : salary,
                'address'   : address,
                'exp'       : ''
            }

            insert_to_mysql(conn, cur, data)
            insert_to_kafka(producer, data)

    driver.quit()

def get_job_from_career_viet(conn, cur, producer) -> None:
    # get total pages
    driver = webdriver.Chrome(service=Service(), options=chrome_options)
    driver.get('https://careerviet.vn/viec-lam/cntt-phan-cung-mang-cntt-phan-mem-c63,1-trang-1-vi.html')

    try:
        pagination = WebDriverWait(driver, 10).until(
            ec.presence_of_element_located((By.CSS_SELECTOR, 'div.job-found-amout h1'))
        )
    except (TimeoutException, NoSuchElementException):
        pagination = None

    if pagination.text:
        total_page = math.ceil(int(pagination.text.split()[0])/50)
    else:
        total_page = 1

    for i in range(1, total_page+1):
        # open web
        driver.get(f'https://careerviet.vn/viec-lam/cntt-phan-cung-mang-cntt-phan-mem-c63,1-trang-{i}-vi.html')
        driver.execute_script("window.scrollBy(0, document.body.scrollHeight)")
        time.sleep(1)

        # get job elements, use presence_of_element_located to await til the element appears
        job_lists_container = WebDriverWait(driver, 20).until(
            ec.presence_of_element_located((By.CSS_SELECTOR, 'div.jobs-side-list'))
        )
        job_lists = job_lists_container.find_elements(By.CSS_SELECTOR, "div.figcaption")

        # query each job in each page
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

            data = {
                'source'    : 'careerviet',
                'position'  : position,
                'company'   : company,
                'salary'    : salary,
                'address'   : address,
                'exp'       : ''
            }

            insert_to_mysql(conn, cur, data)
            insert_to_kafka(producer, data)

    driver.quit()
    return

def get_job_from_it_viec(conn, cur, producer) -> None:
    driver = webdriver.Chrome(service=Service(), options=chrome_options)
    driver.get('https://itviec.com/it-jobs?&page=1')

    try:
        pagination = WebDriverWait(driver, 10).until(
            ec.presence_of_element_located((By.CSS_SELECTOR, 'nav.ipagination.imt-10'))
        )
    except (TimeoutException, NoSuchElementException):
        pagination = None

    if pagination:
        li_list = pagination.find_elements(By.CSS_SELECTOR, 'div.page')
        total_page = int(li_list[-2].text)
    else:
        total_page = 1

    driver.quit()

    for i in range(1, total_page+1):
        # open web
        driver = webdriver.Chrome(service=Service(), options=chrome_options)
        driver.get(f'https://itviec.com/it-jobs?&page={i}')

        # get job elements, use presence_of_element_located to await til the element appears
        job_lists_container = WebDriverWait(driver, 20).until(
            ec.presence_of_element_located((By.CSS_SELECTOR, 'div.col-xl-5.card-jobs-list.ips-0.ipe-0.ipe-xl-6'))
        )
        job_lists = job_lists_container.find_elements(By.CSS_SELECTOR, "div.job-card")

        # query each job in each page
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

            data = {
                'source'    : 'itviec',
                'position'  : position,
                'company'   : company,
                'salary'    : salary,
                'address'   : address,
                'exp'       : ''
            }

            insert_to_mysql(conn, cur, data)
            insert_to_kafka(producer, data)

        driver.quit()

    return

def get_job_from_vietnam_works(conn, cur, producer) -> None:
    driver = webdriver.Chrome(service=Service(), options=chrome_options)
    driver.get('https://www.vietnamworks.com/viec-lam?g=5&page=1')

    try:
        pagination = WebDriverWait(driver, 10).until(
            ec.presence_of_element_located((By.CSS_SELECTOR, 'h1.job-matched span'))
        )
    except (TimeoutException, NoSuchElementException):
        pagination = None

    if pagination.text:
        total_page = math.ceil(int(pagination.text)/50)
    else:
        total_page = 1

    driver.quit()

    for i in range(1, total_page+1):
        # open web
        driver = webdriver.Chrome(service=Service(), options=chrome_options)
        driver.get(f'https://www.vietnamworks.com/viec-lam?g=5&page={i}')
        driver.execute_script("window.scrollBy(0, document.body.scrollHeight)")
        time.sleep(1)

        # get job elements, use presence_of_element_located to await til the element appears
        job_lists_container = WebDriverWait(driver, 20).until(
            ec.presence_of_element_located((By.CLASS_NAME, 'block-job-list'))
        )
        job_lists = job_lists_container.find_elements(By.CSS_SELECTOR, "div.sc-eEbqID.jZzXhN")

        # query each job in each page
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

            data = {
                'source'    : 'vietnamworks',
                'position'  : position,
                'company'   : company,
                'salary'    : salary,
                'address'   : address,
                'exp'       : ''
            }

            insert_to_mysql(conn, cur, data)
            insert_to_kafka(producer, data)

        driver.quit()

def get_job_from_vieclam_24h(conn, cur) -> None:
    driver = webdriver.Chrome(service=Service(), options=chrome_options)
    driver.get('https://vieclam24h.vn/tim-kiem-viec-lam-nhanh?occupation_ids%5B%5D=7&occupation_ids%5B%5D=8&occupation_ids%5B%5D=33&page=1')

    try:
        pagination = WebDriverWait(driver, 10).until(
            ec.presence_of_element_located((By.CSS_SELECTOR, 'div.flex.items-center.gap-4 h1 strong'))
        )
    except (TimeoutException, NoSuchElementException):
        pagination = None

    if pagination.text:
        total_page = math.ceil(int(pagination.text.replace('.', ''))/30)
    else:
        total_page = 1

    for i in range(1, total_page+1):
        # open web
        driver.get(f'https://www.vietnamworks.com/viec-lam?g=5&page={i}')

        # get job elements, use presence_of_element_located to await til the element appears
        job_lists_container = WebDriverWait(driver, 20).until(
            ec.presence_of_element_located((By.CSS_SELECTOR, "main.bg-se-titan-white"))
        )
        job_lists = job_lists_container.find_elements(By.CSS_SELECTOR, "div.flex.flex-col.flex-1.gap-1")
        print('job_lists: ', job_lists)

        # query each job in each page
        for job in job_lists:
            try:
                position = job.find_element(By.CSS_SELECTOR, "div.inline-block.relative.group.align-middle h3").text.strip()
            except NoSuchElementException:
                position = "Not Available"

            # try:
            #     company = job.find_element(By.CSS_SELECTOR, "h3.text-[14px].leading-6.text-[#939295].line-clamp-1").text.strip()
            # except NoSuchElementException:
            #     company = "Not Available"
            #
            # try:
            #     salary = job.find_element(By.CSS_SELECTOR, "div.flex.gap-1.pr-1.pl-[2px].items-center span").text.strip()
            # except NoSuchElementException:
            #     salary = "Not Available"
            #
            # try:
            #     address = job.find_element(By.CSS_SELECTOR, "div.flex.gap-1.pr-1.pl-[2px].items-center.relative.province-tooltip span").text.strip()
            # except NoSuchElementException:
            #     address = "Not Available"

            data = {
                'source'    : 'vieclam24h',
                'position'  : position,
                # 'company'   : company,
                # 'salary'    : salary,
                # 'address'   : address,
                'exp'       : ''
            }

            print(data)

            # insert_to_mysql(conn, cur, data)
            # insert_to_kafka(producer, data)

    driver.quit()

if __name__ == '__main__':
    # producer = SerializingProducer({'bootstrap.servers': 'localhost:9092'})
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="root"
        )

        cur = conn.cursor()

        # get_job_from_top_cv(conn, cur, producer)
        # get_job_from_career_link(conn, cur, producer)
        # get_job_from_career_viet(conn, cur, producer)
        # get_job_from_it_viec(conn, cur, producer)
        # get_job_from_vietnam_works(conn, cur, producer)
        # get_job_from_vieclam_24h(conn, cur)

    except Exception as e:
        print(e)