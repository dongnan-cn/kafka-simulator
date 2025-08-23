# KafkaSimulator

## Project Overview
KafkaSimulator is a Java project used for simulating and managing Kafka consumer groups.

## Environment Preparation
Before starting the project, make sure the following software is installed on your system:
- Docker
- Maven

## Startup Steps
1. Run the following command in the project root directory to start the Kafka service:
   ```
   docker-compose up -d
   ```

2. After the Kafka service has started, run the following command in the project root directory to start the project:
   ```
   mvn clean javafx:run
   ```

## Precautions
- Ensure that Docker and Maven are correctly installed and configured on your system.
- Before starting the Kafka service, check if the configuration in the `docker-compose.yml` file meets your requirements.

