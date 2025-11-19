docker exec -it setup_enviroment-spark-iceberg-1 rm /opt/spark/jars/commons-pool-1.5.4.jar

docker cp jars/spark-token-provider-kafka-0-10_2.12-3.5.5.jar  setup_enviroment-spark-iceberg-1:/opt/spark/jars
docker cp jars/spark-sql-kafka-0-10_2.12-3.5.5.jar setup_enviroment-spark-iceberg-1:/opt/spark/jars
docker cp jars/kafka-clients-3.3.1.CRAC.0.jar  setup_enviroment-spark-iceberg-1:/opt/spark/jars
docker cp jars/hadoop-aws-3.2.2.jar  setup_enviroment-spark-iceberg-1:/opt/spark/jars
#docker cp commons-pool2-2.8.0.jar  setup_enviroment-spark-iceberg-1:/opt/spark/jars
docker cp jars/commons-pool2-2.11.1.jar  setup_enviroment-spark-iceberg-1:/opt/spark/jars
docker cp jars/aws-java-sdk-bundle-1.11.375.jar  setup_enviroment-spark-iceberg-1:/opt/spark/jars
docker cp jars/postgresql-42.4.4.jar  setup_enviroment-spark-iceberg-1:/opt/spark/jars

