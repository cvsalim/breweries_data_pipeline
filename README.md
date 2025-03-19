## **Breweries Case - Data Pipeline**


This documentation covers the construction of a data pipeline related to breweries in various locations. The following architecture was considered for this structure.  
![arqetl](https://github.com/user-attachments/assets/89ac627f-8a4c-49ca-aaa2-d0bef5b549de)
## Architecture  

The pipeline follows the **medallion architecture model**, where the ETL process goes through the **bronze, silver, and gold** layers:  

- **bronze_ingestion.py** – Makes a request via REST API to the data source URL and saves it in the `medallion_data` folder as `bronze_layer.json`.  
- **silver_ingestion.py** – Collects data from `bronze_layer.json`, transforms it into Parquet format, applies data consistency treatments, and saves it in a columnar format, partitioned by city, in the `medallion_data/silver_layer` folder.  
- **gold_ingestion.py** – Collects data from the silver layer, applies transformations to extract business value from the data, and aggregates the number of breweries per state. The results are saved in `medallion_data/gold_layer`.  

Additionally, the following Jupyter notebooks are used for development and testing:  
- **dev_notebook_bronze.ipynb** – Used for testing commands related to data extraction and storage in the bronze layer.  
- **dev_notebook_silver.ipynb** – Used for testing data transformation and storage in the silver layer.  
- **dev_notebook_gold.ipynb** – Used for testing data transformation and storage in the gold layer.  

These files interact with `airflow/dags/dag_pipeline.py`, which contains the **main logic of the solution**. The **Airflow DAG** orchestrates the execution of scripts, testing, alerts, and email notifications via Gmail regarding the pipeline execution status, ensuring efficiency, quality, and failure management.  

---

## **Technology Stack**  

| **Technology**  | **Description**  |
|----------------|----------------|
| **Python**  | Base programming language for the entire code structure.  |
| **PySpark**  | Apache Spark library for distributed processing in Python, enabling efficient handling of large datasets.  |
| **Apache Airflow**  | Pipeline orchestration tool.  |
| **Docker / Docker-Compose**  | Containerization solution for infrastructure standardization and configuration across different operating systems.  |
| **Jupyter Notebook**  | Interactive environment for writing and executing code, visualizing graphs, and adding annotations.  |

---

## **Infrastructure Setup**  

For this solution, **Docker** must be installed, as it is used to create the container where **Airflow** runs. Airflow is responsible for executing the pipeline end-to-end, as well as monitoring and alerting on performance.  

### **Key Files**  
- `docker-compose.yml` – Defines the Airflow setup in Docker.  
- `Dockerfile` – Contains dependencies and container startup configurations.  
- `requirements.txt` – Lists required packages and libraries.  
- `.env` – Contains environment variables.  

### **Installation Steps**  
1. Install and configure **Docker Desktop** for your operating system: [Docker Installation Guide](https://docs.docker.com/desktop/setup/install/windows-install/).  
2. After cloning the repository, open a terminal in the `airflow` folder and start the container with:  
   ```sh
   docker-compose up airflow-init  
   docker-compose up  
## **Accessing Airflow**
After starting the container, you can access **Apache Airflow** by navigating to:
- **URL:** [http://localhost:8080/home](http://localhost:8080/home)
- **Login Credentials:**
  - **Username:** `airflow`
  - **Password:** `airflow`
  - To stop the container, use the following command:
  ```sh
  docker-compose down
## Important Note
To ensure proper functionality, the container must have Java, JDK, and PySpark installed, along with the JAVA_HOME environment variable. In some cases, due to Docker configuration and permission rules, manual installation may be required.

## Manual Installation Steps (if needed)
1. If Java and JDK need to be installed, run the following in your local terminal:
  docker exec -u root -it <airflow-scheduler-container-id> /bin/bash
  apt update && apt install -y default-jdk
2. If JAVA_HOME needs to be set, use:
  export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
  echo "export JAVA_HOME=$JAVA_HOME" >> ~/.bashrc
  source ~/.bashrc
3. If PySpark needs to be installed, use:
  apt update && apt install -y python3-pip
  pip3 install --upgrade pip
  pip3 install pyspark
## **Solution Configuration**
Set Up email Alerts
The pipeline includes a task monitoring service that sends email notifications whenever:
  A task fails, or
  The pipeline completes successfully.
  To enable this feature, configure your email account in DAG parameter:

Run **pyspark_etl_pipeline**
After a few minutes, the pipeline should execute end-to-end, performing all ETL processes.
You should receive a success email notification.
