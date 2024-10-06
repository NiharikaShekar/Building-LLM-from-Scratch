# Building Large Language Model(LLM) from Scratch
#### Author: Niharika Belavadi Shekar
#### UIN :   675234184
#### Email: nbela@uic.edu
#### Instructor: Mark Grechanik


## Project Summary
This project involves building a Large Language Model(LLM) using Cloud computing technologies. This includes text tokenization and vector embeddings computation using Hadoop MapReduce. Therefore, it takes large text corpus, compute the token embeddings along with deploying application on cloud infrastructure, AWS Elastic MapReduce(EMR).
## Prerequisites
To run this project one should ensure that the following are installed on the system(OS:Mac):

- **Java 11**: The project requires JDK 11 to be installed.
- **Scala**: Ensure Scala is installed for writing and executing the code.
- **Apache Hadoop 3.3.6**: A locally installed Hadoop environment is needed for running MapReduce jobs.
- **sbt (Scala Build Tool)**: Used for compiling and building the Scala project.
- **DeepLearning4J**: Library for deep learning in Java/Scala.
- **JTokkit**: Tokenizer library for encoding sentences into tokens.

Make sure all dependencies and libraries are properly configured for the project.

## Steps to Execute the Project

1. **Clone the Repository**
   Clone the project to your local machine using SSH or HTTPS:
   ```bash
   git clone git@github.com:NiharikaShekar/Building-LLM-from-Scratch.git
   ```

2. **Install Dependencies**
   After cloning the repository, navigate to the project directory and install the necessary dependencies:
   ```bash
   cd Building-LLM-from-Scratch
   sbt clean update
   ```

4. **Build the Project**
   Build the project using `sbt assembly` to compile and package it:
   ```bash
   sbt assembly
   # This will create a fat Jar
   ```
5. Run Unit tests and Integration Tests using below command:
   ```
   sbt test
   ```

6. **Run the Project Locally**
   To run the project locally with Hadoop, execute the following command:
   ```bash
   hadoop jar target/scala-2.13/LLM-hw1-assembly-0.1.0-SNAPSHOT.jar input-path output-path
   ```

7. **Verify Output**
   After running the job, you can check the output path for the embeddings generated for the text file.


8. **Create an AWS EMR Cluster**
* Launch an EMR cluster with the following configurations:
* Instance type: Select an instance type based on your specific processing requirements (e.g., m5.xlarge).
* Application: Install Hadoop only.
* Add steps:
   * Tokenizer step:
      * Jar location: [input S3 common JAR path]
      * Jar arguments: tokenizerMain
   * Embedding step:
      * Jar location: [input S3 common JAR path]
      * Jar arguments: embeddingMain
* Submit the job to EMR for processing.

## Additional Notes
- Ensure that Hadoop is properly configured to run in the local environment.
- You can adjust hyperparameters such as the number of epochs, embedding dimensions, or vocabulary size within the code for different training results.

Feel free to contribute to the project or report any issues you encounter.
