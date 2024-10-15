# Advanced Business Analytics

## Overview
Analyze business data through a robust pipeline configured with Apache Kafka, Apache Spark, MongoDB, and Dash.

## Dependencies
- **Apache Kafka**
- **MongoDB** v6.0.17
- **Apache Spark** version 3.5.2
- **Plotly**
- **Dash**
- **Textblob**

## Instructions to Run the Application
Follow these steps to set up and run the application:

1. **Start Zookeeper**
2. **Start Apache Kafka**
3. **Start MongoDB**
4. **Start Apache Spark**

## Main Executables
Ensure to run these scripts in the following order:

- `kafka_topic_producer.py` - Starts the Kafka topic producer.
- `spark_stream_processing.py` - Processes data streaming via Spark.
- `dash_analytics.py` - Launches the Dash analytics dashboard.

## Quick Start
To run all components together, execute the script:
```bash
bash run.sh
