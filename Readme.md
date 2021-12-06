# Spark - ERP (établissements recevant du public) de Bordeaux

## Assignment

[Data](https://opendata.bordeaux-metropole.fr/explore/dataset/bor_erp/table/?dataChart=eyJxdWVyaWVzIjpbeyJjaGFydHMiOlt7InR5cGUiOiJjb2x1bW4iLCJmdW5jIjoiQVZHIiwieUF4aXMiOiJlZmZlY3RpZl9wZXJzb25uZWwiLCJzY2llbnRpZmljRGlzcGxheSI6dHJ1ZSwiY29sb3IiOiJyYW5nZS1BY2NlbnQifV0sInhBeGlzIjoidHlwZSIsIm1heHBvaW50cyI6NTAsInNvcnQiOiIiLCJjb25maWciOnsiZGF0YXNldCI6ImJvcl9lcnAiLCJvcHRpb25zIjp7fX0sInNlcmllc0JyZWFrZG93biI6ImF2ZWNfaGViZXJnZW1lbnQifV0sInRpbWVzY2FsZSI6IiIsImRpc3BsYXlMZWdlbmQiOnRydWUsImFsaWduTW9udGgiOnRydWV9&disjunctive.propriete_ville)

- Persist the data in parquet format with a partition by 'type'
- Calculate the maximum capacity of visitors per street, with or without accommodation
- Save the result of this calculation in a CSV file

## Development

### Packaging

- Compile jar package

```bash
sbt clean package
```

### Test

Run tests using sbt
```
sbt clean test
```

Run a single test class

```
sbt "testOnly *MyTestClass"
```
Example
```
sbt "testOnly *BatchToolBoxSpec"
```

Run a single test

*For exact match rather than substring, use -t instead of -z*
```
sbt "testOnly *BatchToolBoxSpec -- -z test"
```
Example
```
sbt "testOnly *BatchToolBoxSpec -- -z should"
```

### Coverage

Run tests with coverage enabled
```
sbt clean coverage test
```
or if you have integration tests as well
```
sbt clean coverage it:test
```
Generate coverage reports
```
sbt coverageReport
```

### Docs

Generates API documentation for Scala source files
```
sbt doc
```
Generates API documentation for Scala test files
```
sbt test:doc
```
Compile jar package containing API documentation generated from Scala source files
```
sbt clean packageDoc
```
Compile jar package containing API documentation generated from Scala test files
```
sbt clean test:packageDoc
```

---

## Jobs

|    Jobs    | Batch | Streaming |
|:----------:|:-----:|:---------:|
|   Format   | CSV2Parquet |     X     |
| Processing | ERPMaxVisitorByStreet2CSV |     X     |


NB: X = not supported

### Spark Submit CLI

Warning : All there commands are only examples, you will probably need to modify them in order to successful run them.

---

1. CSV 2 Parquet

- Local

```bash
spark-submit --class org.example.batch.CSV2Parquet --master local[*] --properties-file .\resources\config\config.dev.properties ./target/scala-2.12/spark-opendata_2.12-1.0-SNAPSHOT.jar .\path\to\csv\file\bor_erp.csv .\path\to\tsv\file\bor_erp
```

Example

```bash
spark-submit --class org.example.batch.CSV2Parquet --master local[*] --properties-file .\resources\config\config.dev.properties ./target/scala-2.12/spark-opendata_2.12-1.0-SNAPSHOT.jar .\src\main\resources\input\bor_erp.csv .\src\main\resources\test\bor_erp
```

- Cluster

```bash
spark-submit --class org.example.batch.CSV2Parquet --deploy-mode cluster --queue default --properties-file /home/user/spark/config/config.prod.properties ./target/scala-2.12/spark-opendata_2.12-1.0-SNAPSHOT.jar /user/spark/dataset/bor_erp.csv /user/spark/export/bor_erp
```

---

2. ERPMaxVisitorByStreet 2 CSV

- Local

```bash
spark-submit --class org.example.batch.ERPMaxVisitorByStreet2CSV --master local[*] --properties-file .\resources\config\config.dev.properties ./target/scala-2.12/spark-opendata_2.12-1.0-SNAPSHOT.jar .\path\to\csv\file\bor_erp.csv false .\path\to\tsv\file\no_accommodation
```

Example

```bash
spark-submit --class org.example.batch.ERPMaxVisitorByStreet2CSV --master local[*] --properties-file .\resources\config\config.dev.properties ./target/scala-2.12/spark-opendata_2.12-1.0-SNAPSHOT.jar .\src\main\resources\input\bor_erp.csv false .\src\main\resources\test\no_accommodation
```

- Cluster

```bash
spark-submit --class org.example.batch.ERPMaxVisitorByStreet2CSV --deploy-mode cluster --queue default --properties-file /home/user/spark/config/config.prod.properties /home/user/spark/spark-opendata_2.12-1.0-SNAPSHOT.jar /user/spark/dataset/bor_erp.csv false /user/spark/export/no_accommodation
```

---

### Environment

#### HDFS requirements

1. For general purpose

```bash
sudo -u hdfs hdfs dfs -mkdir /user/root
sudo -u hdfs hdfs dfs -chown root /user/root
```

2. For streaming jobs

```bash
sudo -u hdfs hdfs dfs -mkdir -p /hadoop/yarn/local/usercache/root
sudo -u hdfs hdfs dfs -chown root /hadoop/yarn/local/usercache/root
```

Source : [Cloudera](https://community.cloudera.com/t5/CDH-Manual-Installation/Permission-denied-user-root-access-WRITE-inode-quot-user/td-p/4943)

#### Spark utils

[Configuration](https://spark.apache.org/docs/latest/configuration.html)

#### Yarn / HDFS CLI

1. Kill a running job

Ctrl+C isn't enough to kill a running job in command-line. You need to use the following command :

```bash
yarn application -kill application_id
```

Example
```bash
yarn application -kill application_1556787784330_9424
```

2. Copy file from HDFS to local unix file system

```bash
sudo hdfs dfs -copyToLocal /path/to/hdfs/file /path/to/unix/fs/
```

Example
```bash
sudo hdfs dfs -copyToLocal /home/user/spark/extract/extract_20190523 /home/user/spark/extract/extract_20190523
```

Note : Path to unix file system should already exist in order to succeed, meaning you need to create the directories before using the command.

3. Retrieve job logs

```bash
yarn logs -applicationId application_id > /path/to/log/file.log
```

Example
```bash
yarn logs -applicationId application_1556787784330_11541 > /home/user/spark/log/max_visitor_by_street.log
yarn logs -applicationId application_1556787784330_11541 > /home/user/spark/log/max_visitor_by_street.log
```