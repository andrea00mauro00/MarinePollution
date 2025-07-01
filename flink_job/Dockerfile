FROM flink:1.17-scala_2.12
USER root
RUN apt-get update && apt-get install -y python3-pip wget && ln -s /usr/bin/python3 /usr/bin/python
COPY flink_job/requirements.txt .
RUN pip install -r requirements.txt
RUN wget https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.1-1.17/flink-sql-connector-kafka-3.0.1-1.17.jar -O /opt/flink/lib/flink-sql-connector-kafka.jar
RUN wget https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.17/flink-connector-jdbc-3.1.2-1.17.jar -O /opt/flink/lib/flink-connector-jdbc.jar
RUN wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar -O /opt/flink/lib/postgresql-42.6.0.jar
RUN chown -R flink:flink /opt/flink
USER flink
