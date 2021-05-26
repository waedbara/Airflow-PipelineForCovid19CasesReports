# Building Airflow Pipeline to generate reports for Jordan from 2020 to now,

# Data Source from COVID-19 Data Repository by the Center for Systems Science and Engineering (CSSE) at Johns Hopkins University up to now.

# Airflow Day

![alt Airflow Day ](dags/airflowDag.png)

# Jordan Scoring Report

![alt Jordan Scoring Report](dags/Jordan_scoring_report.png)

# Save Reports In Postgres database

![alt Jordan Scoring Report](dags/JoReport.png)
![alt Jordan Scoring Report](dags/JoReportNotScaled.png)

# How to Run Project

> mkdir dags logs plugins pgadmin postgres-db-volume

> docker-compose up airflow-init
> docker-compose up

> pgAdmin : http://localhost:8000/

> Airflow : http://localhost:8080/
