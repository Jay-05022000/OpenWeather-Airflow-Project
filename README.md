                                                           OpenWeather-Airflow-Project
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white) ![Visual Studio Code](https://img.shields.io/badge/Visual%20Studio%20Code-0078d7.svg?style=for-the-badge&logo=visual-studio-code&logoColor=white) ![AWS](https://img.shields.io/badge/AWS-%23FF9900.svg?style=for-the-badge&logo=amazon-aws&logoColor=white) ![Power Bi](https://img.shields.io/badge/power_bi-F2C811?style=for-the-badge&logo=powerbi&logoColor=black) 
 

Objective:

The primary goal of this project is to develop a dashboard or webpage that allows users to access current and future weather forecast information for a desired location conveniently. Users will be able to obtain real-time weather updates, 12-hour forecasts, and 8-day weather forecasts for selected locations with a single click.

Target Users:

The main users of this project are myself and my family members. Therefore, the weather insights will be provided for the following locations:

1) Windsor, Ontario, Canada: My current location.
2) Ottawa, Ontario, Canada: My sister's location.
3) Ahmedabad, Gujarat, India: My hometown and my parents' current location.
4) Perth, Australia: My wife's current location.

Data Source:

The weather data will be retrieved from the OpenWeather API.
Link: OpenWeather

Workflow:

![image](https://github.com/Jay-05022000/OpenWeather-Airflow-Project/assets/110780565/79e269c6-0e52-43af-8b1c-78fc789ccbce)

Tools Used:

1) Visual Studio Code: To develop Python scripts.
2) Apache Airflow: To orchestrate the complete ETL pipeline, in conjunction with WSL2 with Ubuntu OS locally.
3) AWS Glue: To crawl CSV data available in the S3 bucket and create Athena tables for data analysis.
4) Power BI Service: To create an interactive visualization report that can be refreshed automatically and shared with users.

Project Learnings:

1)  Setting environment variables in the local system to store credentials for system-wide or different IDE usage.
    Example: Stored AWS credentials in the Windows system to use them in Jupyter Notebook to connect to S3.
2)  Hosting or setting up an Apache Airflow project locally using WSL2 with Ubuntu system.


Output:

A successful run of this project will result in an email being sent to all users. This email will include information about the DAG run and a link that allows users to access the real-time weather dashboard for their chosen location.

![image](https://github.com/Jay-05022000/OpenWeather-Airflow-Project/assets/110780565/587dce85-ce9a-450d-ab79-2190efa0bf53)

Interactive Weather Dashboard Accessible to user.

![image](https://github.com/Jay-05022000/OpenWeather-Airflow-Project/assets/110780565/0e55685a-6990-4533-8481-cdcbea062520)

Challenges:

1) Error in Airflow while executing a task involving the Email Operator.

   Explanation: Using Airflow installed locally on WSL with Ubuntu OS, connected to home Wi-Fi. Running a DAG causes a network error during the Email Operator task.
   Reason: Potential security settings in the Wi-Fi blocking outgoing traffic to the SMTP server.
   Solution: Used personal networks like mobile hotspots to bypass security restrictions while running the DAG. To permanently resolve the issue, configure the Wi-Fi     router to allow outgoing SMTP traffic.
   
3) Inconsistent and unformatted data for Daily_Weather_Forecast in the Athena table created by the Glue Crawler.

   Explanation: The Daily_Weather_Forecast.json file has a column named "Summary" with some values containing commas, causing column value misinterpretation by the       crawler during Athena table creation.
   Solution: Modified the transformation.py script to remove unwanted commas from the Summary column of the daily forecast data.

ETL Automation:

The entire ETL process, from data extraction to sending an email notification about the DAG run status with a Power BI dashboard attachment, can be automated in different stages:

1) Data Extraction, Transformation, and Storage to S3: Automated using Airflow.
2) Updating Athena Tables to Have the Latest Data: Automated by scheduling crawlers to crawl source-cleaned files and update/create Athena tables at the desired time     and frequency.
3)  Updating Data in Power BI: Automated by configuring scheduled refreshes in the Power BI service to ensure the dashboard shows the most current data.

A Microsoft 365 Business Basic account is used to create a professional email and log into the Power BI service. The dashboard is published online using Power BI Report Server, allowing others to access it via a shared link without needing Power BI on their local systems. The dashboard is also accessible on mobile devices.
