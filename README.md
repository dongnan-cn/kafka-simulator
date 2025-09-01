# KafkaSimulator

## Project Overview
KafkaSimulator is a JavaFX-based desktop application for simulating and visualizing Apache Kafka features. It provides an intuitive graphical interface to interact with Kafka clusters, making it ideal for learning, testing, and demonstrating Kafka concepts.

### Key Features
- **Cluster Management**: Connect to and manage Kafka clusters
- **Topic Management**: Create, delete, and view topics with configurable partitions and replication factors
- **Producer Simulation**: 
  - Create multiple producer instances
  - Configure producer settings (acks, batch.size, linger.ms)
  - Send messages manually or automatically
  - Support for different message types (String, JSON)
  - Multi-tab interface for managing producers for different topics
- **Consumer Simulation**:
  - Create multiple consumer instances with different group IDs
  - Configure consumer settings (auto.offset.reset, auto commit)
  - View received messages in real-time
  - Manual offset commit functionality (when auto commit is disabled)
  - Visualize partition assignments
- **Visual Monitoring**: Real-time display of message flows and consumer group activities

## Environment Preparation
Before starting the project, make sure the following software is installed on your system:
- Docker (for running Kafka cluster)
- Maven (for building and running the application)
- Java JDK 11 or higher

## Startup Steps
1. Run the following command in the project root directory to start the Kafka service:
   ```
   docker-compose up -d
   ```
   This will start a Kafka cluster with Zookeeper using the configuration in `docker-compose.yml`.

2. To stop the Kafka service:
   ```
   docker-compose down
   ```

3. To stop the Kafka service and remove all volumes:
   ```
   docker-compose down --volumes
   ```

4. After the Kafka service has started, run the following command in the project root directory to start the application:
   ```
   mvn clean javafx:run
   ```

## Usage Guide

### Connecting to Kafka Cluster
1. Launch the application
2. Enter the Kafka bootstrap servers (default: `localhost:19092`)
3. Click "Connect" to establish a connection

### Managing Topics
1. After connecting, view all existing topics in the topic list
2. Create a new topic by specifying the topic name, number of partitions, and replication factor
3. Delete existing topics as needed
4. Refresh the topic list to see the latest changes

### Using Producers
1. Select a topic from the dropdown list
2. Configure producer settings (acks, batch.size, linger.ms)
3. For manual sending:
   - Enter message key and value
   - Click "Send Message"
4. For automatic sending:
   - Configure auto-send settings (messages per second, data type)
   - Click "Start Auto Send"
   - Monitor the message count
   - Click "Stop Auto Send" when finished

### Using Consumers
1. Enter a consumer group ID
2. Select topics to subscribe to
3. Configure consumer settings (auto.offset.reset, enable auto commit)
4. Click "Create Consumer Group"
5. View received messages in real-time
6. If auto commit is disabled, use the "Manual Commit Offset" button to commit offsets manually

## Precautions
- Ensure that Docker and Maven are correctly installed and configured on your system.
- Before starting the Kafka service, check if the configuration in the `docker-compose.yml` file meets your requirements. By default, we use local port 19092 to connect to the Kafka container in Docker.
- When using manual offset commit, ensure that the consumer group is active and running.
- The application is designed for development and testing purposes, not for production use.

## Project Structure
The project follows a layered architecture:
- **UI Layer**: JavaFX-based user interface
- **Business Logic Layer**: Kafka client operations and business rules
- **Data/Model Layer**: Data structures for inter-layer communication

## Future Enhancements
- Support for Avro message format
- Integration with Schema Registry
- Advanced monitoring metrics (throughput, latency)
- Message browsing and search functionality
- Enhanced visualization of consumer group rebalancing

## Contributing
This project is open for contributions. If you would like to add new features or improve existing ones, please follow the guidelines in the project plan (plan.md).
