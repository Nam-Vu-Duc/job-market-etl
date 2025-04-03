IT JOBS MARKET ETL
===============================

I implemented a pipeline to scrape, process, and analyze the job market. First, I used Selenium to scrape all the newest IT jobs from the top 5 recruitment websites, clean them, and store them in MySQL. Then, I streamed these data using Kafka, processed them using Spark, and stored the result in PostgreSQL. Finally, I visualized the result using Superset. All of this was scheduled by Airflow. 

## System Architecture
![image](https://github.com/user-attachments/assets/48f5dc11-cbb6-4662-9037-67f9bc8a76b3)

## System Components
- **scrape.py**: This file scrapes all IT job data from top 5 recruit websites, cleans these data, save to mysql and produce new topic to kafka.
- **process.py**: This file consume data from kafka, process these data using spark, then save to postgesql and produce new topics to kafka.
- **visualize.py**: This file consume data from kafka, together with data from postgres to generate dashboard to analyze.

## Setting up the System
This Docker Compose file allows you to easily spin up Zookkeeper, Kafka and Postgres application in Docker containers. 

### Prerequisites
- Python 3.9 or above installed on your machine
- Docker Compose installed on your machine
- Docker installed on your machine

### Steps to Run
1. Clone this repository.
2. Navigate to the root containing the Docker Compose file.
3. Run the following command:

```bash
docker-compose up -d
```
This command will start Zookeeper, Kafka and Postgres containers in detached mode (`-d` flag). Kafka will be accessible at `localhost:9092` and Postgres at `localhost:5432`.

##### Additional Configuration
If you need to modify Zookeeper configurations or change the exposed port, you can update the `docker-compose.yml` file according to your requirements.

### Running the App
1. Install the required Python packages using the following command:

```bash
pip install -r requirements.txt
```

2. Creating the required tables on Postgres and generating voter information on Kafka topic:

```bash
python scrape.py
```

3. Consuming the voter information from Kafka topic, generating voting data and producing data to Kafka topic:

```bash
python process.py
```

4. Consuming the voting data from Kafka topic, enriching the data from Postgres and producing data to specific topics on Kafka:

```bash
python visualize.py
```

## Demo
### Scraping data

### Process data

### Visualize data





