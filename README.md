# SparkStreamingApplication

- This project demonstrates the application of PySpark in processing Big Data. It contains **2 parts**: 
  + **Part 1** develops **PySpark Machine Learning Pipelines** compressing the entire process of developing Machine Learning models, which includes Data Preparation,  Model Training, and Model Evaluation. 
  + **Part 2** establishes **Kafka streams** of events via which, different services can communicate with each other. A **Data Streaming Application** is developed using **PySpark** to monitor the **Kafka events**, reformat and load them into tabular dataframes, and then pass this data into **Machine Learning Pipelines** to generate predictions. 

- The following diagram describes the architecture of this project.
![alt text](https://user-images.githubusercontent.com/35318567/138625028-775b09bd-db99-4f7e-9eeb-51262a64c82d.png)

- Data used in this project contains insights related to the **performance of a computer**, including metrics of its Memory and Processor. Data is stored in the form of **CSV files** and processed using **PySpark**. This data is used to train Machine Learning models that predict Cyber Attacks aim on the computer.

- After data has been loaded into the Spark application, the following processes would be carried out:
  + Data Wrangling
  + Data Visualization
  + Feature Extraction
  + Training Machine Learning models
  + Models Evaluation

### Part 1: PySpark Machine Learning Pipelines
1. Data is loaded from CSV files using **PySpark** before being split into training and testing datasets. For each of them, the ratio of Attack and Non-attack events are rebalanced to be 1:2.
2. Data exploration is carried out using **PySpark SQL functions**.
3. **matplotlib** and **seaborn** are then used to visualize the data and support Feature Selection.
4. After Feature Selection, data is passed through **PySpark Machine Learning Pipelines**, each of which contains the following stages:
  - **StringIndexer**: Indexing categorical features
  - **OneHotEncoder**: Encoding the categorical features
  - **VectorAssembler**: Assemble the features (both numerical and categorical) into one single Feature column
  - **StandardScaler**: Scale the Feature column
  - Machine Learning models: Either Decision Tree (**DecisionTreeClassifier**) or Gradient Boosted Tree (**GBTClassifier**).
5. The pipelines are used to process data then train and evaluate the Machine Learning models.
6. Models are evaluated by their Accuracy, Precision, Recall, F1-Score, and AUC score and ROC curve.


### Part 2: PySpark Data Streaming Application
- To ensure that the Kafka server is started, please use the following commands:
```
sudo systemctl start zookeeper
sudo systemctl start kafka
```
- There are 2 event Producers that publish data to the Kafka stream: Memory Producer (_memory_ topic) and Process Producer (_process_ topic).
- There are 2 event Consumers that subscribe data from the Kafka stream and generate real-time visualization of the events: Memory Consumer (_memory_ topic) and Process Consumer (_process_ topic). 
- There is a PySpark Streaming Application that process the Kafka events and use them as the input for Machine Learning Pipelines predicting possible cyber attacks. The number of potential attacks is visualized using a real-time graph.
  + Kafka events are loaded into tabular dataframes before being pre-processed. This data can be persisted in `.parquet` files.
  + Machine Learning models are loaded and used to generate predictions of potential attacks using the Kafka events. The predicitons data can also be persisted in `.parquet` files.
  + `matplotlib` is used to generate real-time visualization of the predicitons.
