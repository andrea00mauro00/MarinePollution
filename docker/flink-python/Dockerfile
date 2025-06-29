# Usa l'immagine ufficiale Flink per la versione corretta
FROM flink:1.17-scala_2.12

# Passa temporaneamente a root per installare i pacchetti
USER root

# Installa le dipendenze di sistema (Python, pip, wget)
RUN apt-get update && apt-get install -y python3 python3-pip wget \
    && ln -s /usr/bin/python3 /usr/bin/python \
    && rm -rf /var/lib/apt/lists/*

# --- INSTALLA LE DIPENDENZE PYTHON (SINTASSI CORRETTA) ---
# Installa apache-flink e il suo "extra" per il connettore JDBC.
RUN pip install "apache-flink[jdbc]==1.17.2"

# --- INSTALLA LE DIPENDENZE JAVA (I FILE JAR) ---
# 1. Il "fat JAR" per Kafka
RUN wget https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.1-1.17/flink-sql-connector-kafka-3.0.1-1.17.jar -O /opt/flink/lib/flink-sql-connector-kafka.jar
# 2. Il connettore Flink per JDBC
RUN wget https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.17/flink-connector-jdbc-3.1.2-1.17.jar -O /opt/flink/lib/flink-connector-jdbc.jar
# 3. Il driver JDBC per PostgreSQL
RUN wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar -O /opt/flink/lib/postgresql-42.6.0.jar

# Assegna i permessi corretti
RUN chown -R flink:flink /opt/flink \
    && chmod -R 775 /opt/flink/conf \
    && chmod -R 775 /opt/flink/log

# Torna all'utente di default non-root 'flink'
USER flink
