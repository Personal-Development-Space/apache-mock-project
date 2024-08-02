hadoop fs -ls /

hadoop fs -ls hdfs://localhost:9000/     

hadoop fs -mkdir /nguyenkieubaokhanh/apache-mock-project

hdfs getconf -confKey fs.defaultFS

/home/hadoopusr/hadoop/etc/hadoop

hadoop fs -chown nguyenkieubaokhanh /nguyenkieubaokhanh

spark-submit --master "local[*]" --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,mysql:mysql-connector-java:5.1.49,io.delta:delta-spark_2.12:3.2.0 --files /home/nguyenkieubaokhanh/nguyenkieubaokhanh/CODE/apache-mock-project/realtime_data_processing/datamaking_app.conf /home/nguyenkieubaokhanh/nguyenkieubaokhanh/CODE/apache-mock-project/realtime_data_processing/realtime_data_processing.py

kafka-topics.sh --create --topic order-events --bootstrap-server localhost:9092
kafka-topics.sh --create --topic output-topic --bootstrap-server localhost:9092

Let kafka listen to the order-events topic:
kafka-console-consumer.sh --topic order-events --from-beginning --bootstrap-server localhost:9092
