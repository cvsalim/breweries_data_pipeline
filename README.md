## **Breweries Case - Data Pipeline**


This documentation covers the construction of a data pipeline related to breweries in various locations. The following architecture was considered for this structure.  
![arqetl](https://github.com/user-attachments/assets/7383540e-2b4a-4142-a261-b1aafa7aa799)

## Architecture  

The pipeline follows the **medallion architecture model**, where the ETL process goes through the **bronze, silver, and gold** layers:  

- **bronze_ingestion.py** – Makes a request via REST API to the data source URL and saves it in the `medallion_data` folder as `bronze_layer.json`.  
- **silver_ingestion.py** – Collects data from `bronze_layer.json`, transforms it into Parquet format, applies data consistency treatments, and saves it in a columnar format, partitioned by city, in the `medallion_data/silver_layer` folder.  
- **gold_ingestion.py** – Collects data from the silver layer, applies transformations to extract business value from the data, and aggregates the number of breweries per state. The results are saved in `medallion_data/gold_layer`.  

Additionally, the following Jupyter notebooks are used for development and testing:  
- **dev_notebook_bronze.ipynb** – Used for testing commands related to data extraction and storage in the bronze layer.  
- **dev_notebook_silver.ipynb** – Used for testing data transformation and storage in the silver layer.  
- **dev_notebook_gold.ipynb** – Used for testing data transformation and storage in the gold layer.  

These files interact with **airflow/dags/dag_pipeline.py**, which contains the **main logic of the solution**. The **Airflow DAG** orchestrates the execution of scripts, testing, alerts, and email notifications via Gmail regarding the pipeline execution status, ensuring efficiency, quality, and failure management.  

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
   docker-compose up --build  
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
   
  export JAVA_HOME=$(dirname $ ( dirname $(readlink -f $ ( which java))))
  
  echo "export JAVA_HOME=$JAVA_HOME" >> ~/.bashrc
  
  source ~/.bashrc
  
3. If PySpark needs to be installed, use:

  apt update && apt install -y python3-pip
  
  pip3 install --upgrade pip
  
  pip3 install pyspark

## **Solution Configuration**
**Notify tasks:** The pipeline includes a task monitoring service that sends email notifications whenever:
  A task fails, or
  The pipeline completes successfully.
  To enable this feature, configure your email account in **airflow/dags/dag_pipeline.py** parameter 'send_email'.
  You should receive an email as the following
  
![failtask](https://github.com/user-attachments/assets/ff583c48-8394-47af-a0c4-acde0abffce0)

**(If needed)** To run the success notification task is necessary to set up the Connection smtp. To do this, go to connections in airflow and add the connection as following, also, add the token in the password field.
![image](https://github.com/user-attachments/assets/a0d1c603-d3a8-408a-9872-116cb1ef924b)

Run **pyspark_etl_pipeline**
After a few minutes, the pipeline should execute end-to-end, performing all ETL processes.
You should receive a success email notification.
There is the **result** of the orchestration and the Data curated is save in `medallion_data/gold_layer`.
![unnamed](https://github.com/user-attachments/assets/d35e29f0-f031-4487-9459-a81f11d4a210)

## **Next Steps**
As the next steps, it is recommended to implement solutions focused on generating business value based on the data from the gold layer. This data can be used to create reports that provide valuable insights for decision-makers, fostering a data-driven culture.

One possible approach is to deploy the Metabase application using Docker, a tool capable of connecting to the data source and enabling ad hoc analysis and dashboard creation.

## **Cloud Architectures**
The solution was designed in an agnostic and local manner; however, there is the possibility of replicating this solution in Cloud Computing platforms. This would provide benefits such as greater scalability, additional functionalities, resilience, and other key aspects relevant in a real business environment.

For this reason, below are examples of architectures for this solution in AWS and Azure:

**Data Pipeline on AWS**
For this scenario in AWS, a Lambda function can be used to call the API and save the data into an S3 bucket. EMR functions can process the data and utilize Spark for the silver and gold layers, storing the results in S3 buckets. The entire workflow can be orchestrated using Step Functions or Airflow.

CloudWatch can be integrated with Lambda and SNS to provide alerts and notifications for error management. Additionally, data analysis can be performed using Athena, and dashboards can be created with QuickSight.
![awsawsrc](https://github.com/user-attachments/assets/48f5e4fe-bc74-45ed-b34b-97e24703c019)


**Data Pipeline on Azure**
For this scenario, following a similar approach, Azure Functions or Azure Container Instances (ACI) can be used for the bronze layer. Azure Data Factory can handle orchestration, while Databricks can process data in the subsequent layers. Azure Storage can be used for data storage.

For monitoring and alerts, Azure Monitor and Azure Alerts can be utilized.
![azerre](https://github.com/user-attachments/assets/3f73de79-7100-4767-bdda-adc766711911)

