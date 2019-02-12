- Authors: Gkoutziouli Dimitra & Gkouzionis Ioannis

- Parameters needed for running the Count-Min & Bloom-Filters project

1. -p X : parallelism with value X
2. --class Y : where Y is name of the main class -> count_min.count_min.App /path/to/the/jar/file.jar
3. --sketch Z : where Z is the option for running either the count-min (sketch=1) or the bloom-filter (sketch=2) 
4a. --inputCM name : where name is the path to the dataset used as the input for the count-min
4b. --inputBF name : where name is the path to the dataset used as the input for the bloom-filter
5a. --inputQCM name : where name is the path to the query dataset used as the queries for the count-min
5b. --inputQBF name : where name is the path to the query dataset used as the queries for the bloom-filter
6a. --outputCM name : where name is the path to the output folder used for writing the output of the count-min
6b. --outputBF name : where name is the path to the output folder used for writing the output of the bloom-filter

-Below you can find an example for running the project of count-min & bloom-filters using inputs and outputs from the HDFS environment:

./bin/flink run --class count_min.count_min.App /home/hadoop/cmbf/count-min.jar --inputCM hdfs://hadoopsrv:9000/user/hduser/cmbf/inputCM.txt --inputQCM hdfs://hadoopsrv:9000/user/hduser/cmbf/inputQCM.txt --outputCM hdfs://hadoopsrv:9000/user/hduser/cmbf/CMoutput

- Important note !!!

If you want to use input and output from HDFS use the following:

--input hdfs://nnhost:nnport/path/to/the/input/file.txt
--inputQueries hdfs://nnhost:nnport/path/to/the/input/queriesFile.txt
--output hdfs://nnhost:nnport/path/to/the/output/directory