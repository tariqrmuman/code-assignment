# F1 Pipeline with Apache Beam

## Overview

This Apache Beam pipeline is designed to process F1 lap time data and calculate the top three F1 drivers by average lap time, as well as their fastest lap times. It reads data from a CSV file, performs data transformations, and writes the results to an output file in JSON format.

## Prerequisites

Before running the F1 pipeline, make sure you have the following set up:

- Apache Maven (to build and run the pipeline)
- Java Development Kit (JDK) installed
- A CSV file containing F1 lap time data named `laptimes.csv`
- Apache Beam dependencies included in your project's `pom.xml`

## Running the Pipeline

### Building the Project

You can build the project using Maven. Navigate to the project directory containing the `pom.xml` file and execute the following command:

```bash
mvn clean install
```
### Running the Pipeline Locally

To run the pipeline on the Direct Runner (local mode), execute the following command:

```bash
mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.F1Pipeline -P direct-runner
```
This command will start the pipeline and process the data locally.

### Input Data
The pipeline reads input data from the laptimes.csv file, which should be present in the same directory as your project. Ensure that your CSV file has a header with the following columns:

```csv
Driver,Time
```

### Output Data
The pipeline will produce an output file in JSON format containing the top three F1 drivers by average lap time, along with their fastest lap times. The output file will be stored in a current directory named "output" with a ".json" extension.

### Unit Testing
To run unit tests for the pipeline, execute the following command:

```bash
mvn test
```
Ensure that your tests cover various aspects of the pipeline's functionality.