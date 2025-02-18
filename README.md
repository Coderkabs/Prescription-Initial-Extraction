
# PRESCRIPTION PAYLOAD EXTRACTION

## Overview
Prescription  extraction  is a data streaming project that utilizes Flink to consume data  from a prescriptions-ack using  kafka  and route  data to dynamic topics..

## Project Structure

```
CarePro-pipelines/
├── src/main/java/org/example/
│   ├── Main.java # implements the  business logic
│   ├── ConsumeManager.java # implements  the kafka  server configuration
├── pom.xml
└── README.md
```

## Technologies Used
- **Apache Flink**: For real-time stream processing
- **Apache Kafka**: As a message broker
- **PostgreSQL**: As the target database
- **Jackson (FasterXML)**: For JSON parsing
- **Maven**: For dependency management and project build

## Setup Instructions

### Prerequisites
Ensure you have the following installed:
- Java 11 or higher
- Maven 3.6+
- PostgreSQL database
- Kafka cluster with configured topics

Configuration

Kafka Connection Settings (in `CosumeManager.java`)
Modify the following properties in `getKafkaProperties()` to match your Kafka setup:
```java
props.setProperty("bootstrap.servers", "");
props.setProperty("group.id", "");
props.setProperty("sasl.jaas.config", "");
```



## Building and Running

### Build the Project
```sh
mvn clean package
```

### Run the Application
```sh
java -jar target/Prescription-Extracts-1.0-SNAPSHOT.jar
```

## Code Explanation

### `Main.java`
- **Consumes Kafka messages** from the `prescriptions` topic.
- **Configure kafka producer.
- **produce consumed  data to dynamic  topics with the  hmisCode  present in the prescription-Ack topic.
-** Filter necessary  field from the payload 
### `ConsumeManager.java`
- Defines Kafka consumer properties and creates a Flink Kafka consumer.

### `pom.xml`
- Contains project dependencies such as Flink, Kafka, PostgreSQL JDBC, and Jackson.

## Routed  Payload

```json
{

  "msh": {

    "timestamp": "2025-01-27 11:32:43",

    "sendingApplication": "sc+",

    "receivingApplication": "elmis",

    "messageId": "5a903538-a8f1-4fd7-a987-0026f8b36843",

    "hmisCode": "50060243",

    "messageType": "prescription"

  }
  

}
```
## Troubleshooting
### Common Issues
- **Kafka Connection Issues**: Ensure Kafka brokers and authentication settings are correct.
- **Dependency Conflicts**: Run `mvn dependency:tree` to check for conflicting dependencies.

## Future Enhancements
- Add monitoring and logging mechanisms.



