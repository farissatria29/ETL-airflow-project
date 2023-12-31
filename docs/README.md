# Airflow ETL Project

This project uses Apache Airflow to orchestrate an ETL (Extract, Transform, Load) process for moving data from one MySQL database to another, where the destination database serves as a data warehouse.

## Overview

Apache Airflow is an open-source platform designed to programmatically author, schedule, and monitor workflows. In this project, we leverage Airflow to automate the ETL process, ensuring the efficient extraction, transformation, and loading of data from a source MySQL database to a target MySQL database.

## Project Structure

The project is structured as follows:

- **dags**: Contains the Apache Airflow DAG (Directed Acyclic Graph) definition file.
- **scripts**: Holds the ETL script that extracts, transforms, and loads data.
- **docker**: Docker configuration files to run Airflow in a Docker container.
- **docs**: Project documentation or README files.
- **logs**: Folder to store Airflow logs.
- **output**: Folder to store the output generated by the ETL process.

## DAG Configuration

The DAG defines the workflow, schedule, and dependencies.

## ETL Script

The ETL script contains the logic for extracting, transforming, and loading data.
