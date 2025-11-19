#!/bin/bash
rm /opt/spark/jars/commons-pool-1.5.4.jar

cp /home/iceberg/jars/spark-token-provider-kafka-0-10_2.12-3.5.5.jar  /opt/spark/jars
cp /home/iceberg/jars/spark-sql-kafka-0-10_2.12-3.5.5.jar /opt/spark/jars
cp /home/iceberg/jars/kafka-clients-3.3.1.CRAC.0.jar /opt/spark/jars
cp /home/iceberg/jars/hadoop-aws-3.2.2.jar /opt/spark/jars
cp /home/iceberg/jars/commons-pool2-2.11.1.jar /opt/spark/jars
cp /home/iceberg/jars/aws-java-sdk-bundle-1.11.375.jar /opt/spark/jars
cp /home/iceberg/jars/postgresql-42.4.4.jar /opt/spark/jars

