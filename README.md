JOB MARKET ETL
===============================

I implemented a pipeline to scrape, process, and analyze the job market by each job's address, location and exp.
![image](https://github.com/user-attachments/assets/b40bca06-6252-470f-9cdc-6e7398b9322c)
## System Components
- **create_table.py**: create required tables in mysql and postgres for further workflow
- **scrape_data**: scrape jobs from website using Selenium and clean these data using Pandas, then save cleaned data to mysql and produce to kafka topic 'jobs-topic'
- **process_data**: consume data from topic 'jobs-topics, process these data using spark, then produce to new kafka topics 'address_report', 'location_report', 'exp_report'
- **visualize_report**: consume data from topics 'address_report', 'location_report', 'exp_report' and save them to postgres for superset visualization
- **send_email**: Read data from postgres and send email to user

## Prerequisites
- Python 3.9 or above installed on your machine
- Docker Compose installed on your machine
- Docker installed on your machine

## Steps to Run
1. Clone this repository.

2. Config Requirements and Dockerfile
### Build Image with requirement installation.
```bash
docker build -t airflow-webscraping .
```

### Build Docker container
```bash
docker-compose up -d
```
This command will start Zookeeper, Kafka, Airflow and Postgres containers in detached mode (`-d` flag). 
Kafka will be accessible at `localhost:9092`, Postgres at `localhost:5432`, Superset at `localhost:8088` and Airflow at `localhost:8080`

3. Config Superset Docker:
### Enter the repository you just cloned
```bash
cd superset
```

### Set the repo to the state associated with the latest official version
```bash
git checkout tags/4.1.2
```

### Fire up Superset using Docker Compose
```bash
docker compose -f docker-compose-image-tag.yml up
```

### Additional Configuration
If you need to modify configurations or change the exposed port, you can update the `docker-compose.yml` file according to your requirements.

## Running the App
1. Install the required Python packages using the following command:
```bash
pip install -r requirements.txt
```

2. Creating required tables on MySQL and Postgres:
```bash
python create_table.py
```

3. Srcape and clean data, save to mysql and produce to kafka:
```bash
python scrape_data.py
```

4. Consume data from kafka and process, then produce to new kafka topic:
```bash
python process_data.py
```

5. Consume data and save to postgresql for superset visualization
```bash
python visualize_report.py
```

6. Read data from postgres again to send email to user:
```bash
python send_email.py
```

### Demo





