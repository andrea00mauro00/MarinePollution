FROM flink:1.17

# Installa Python e pip
RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-dev && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Installa PyFlink e altre dipendenze
RUN pip3 install apache-flink==1.17.0 \
    kafka-python \
    psycopg2-binary \
    shapely \
    numpy \
    pandas \
    scikit-learn \
    matplotlib

# Verifica l'installazione
RUN python -c "from pyflink.datastream import StreamExecutionEnvironment; print('PyFlink installato correttamente')"

# Installa driver JDBC per PostgreSQL
RUN wget https://jdbc.postgresql.org/download/postgresql-42.5.1.jar -O /opt/flink/lib/postgresql-42.5.1.jar