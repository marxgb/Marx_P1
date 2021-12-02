#Revature Project 1: Cardiovascular Disease

A program that analyzes a database to figure out what correlations does certain physical/habits play in the odds of one developing cardiovascular diseases. The project was 
developed using Scala, Spark/Hive, MySQL and Hadoop.

### Data Source

The source for the election data used in this project comes from Kaggle
The website can be found here: https://www.kaggle.com/sulianova/cardiovascular-disease-dataset

### Technology Stack

This application makes use of the following technologies:

Technology | Version
---------- | -------
Hadoop | 3.2
Hive | 3.1.2
Intellij IDEA (Community Edition) | 2021.2.3
MySQL | 8.0.26
SBT | 1.5.5
Scala | 2.12.15
Spark | 3.2.0

### Run

Install the above technologies and clone the repository.

Ensure that the following environmental variables for the MySQL user account are set:

```
MYSQL_USER
MYSQL_PASSWORD
```

In the project directory run the following:

```
sbt run
```


