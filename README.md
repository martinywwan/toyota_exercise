# Toyota Exercise

## Overview
This README provides instructions for running the ToyotaExercise in your local machine

## Prerequisites
Before running the application, ensure that you have the following installed:
- Java 8+
- **Apache Spark**

## Building the Application
If you need to build the application from source, follow these steps:

1. Clone the repository
2. `mvn clean package` - This will create a JAR file

## Running the Application
You can run the Spark application using the spark-submit command.

$SPARK_HOME/bin/spark-submit \
  --class <main-class-name> \
  --master <master-url> \
  --deploy-mode <deploy-mode> \
  <path-to-jar> \
  [application-arguments]

e.g.
$SPARK_HOME/bin/spark-submit \
  --class com.martinywwan.ToyotaExercise \
  --master local[*] \
  target/ToyotaExercise-1.0-SNAPSHOT.jar
