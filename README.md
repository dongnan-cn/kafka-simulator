# KafkaSimulator

## Project Overview
KafkaSimulator is a Java project used for simulating and visualizing Kafka features. You can manage:
- topics
- partitions
- produce messages
- consume messages
- consumer groups
- etc.

## Environment Preparation
Before starting the project, make sure the following software is installed on your system:
- Docker
- Maven

## Startup Steps
1. Run the following command in the project root directory to start the Kafka service:
   ```
   docker-compose up -d
   ```
2. To stop the Kafka service:
   ```
   docker-compose down
   ```
3. To stop the Kafka service and remove all volumes:
   ```
   docker-compose down --volumes
   ```

2. After the Kafka service has started, run the following command in the project root directory to start the project:
   ```
   mvn clean javafx:run
   ```

## Precautions
- Ensure that Docker and Maven are correctly installed and configured on your system.
- Before starting the Kafka service, check if the configuration in the `docker-compose.yml` file meets your requirements. By default, we use local port 19092 to connect to the Kafka container in Docker.
