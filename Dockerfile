#FROM python:3.8-alpine
#RUN pip install --upgrade pip

#COPY requirements.txt .
#RUN pip install -r requirements.txt

# builder step used to download and configure spark environment
FROM openjdk:11.0.11-jre-slim-buster as builder

# Add Dependencies for PySpark
RUN apt-get update && apt-get install -y curl vim wget software-properties-common ssh net-tools ca-certificates python3 python3-pip pkg-config libcairo2-dev libjpeg-dev libgif-dev
#RUN add-apt-repository ppa:deadsnakes/ppa
#RUN apt-get install -y python3.8 python3-pip

#RUN update-alternatives --install "/usr/bin/python" "python" "$(which python3)" 1
#RUN apt-get update && \
#    apt-get install -y wget vim software-properties-common ssh net-tools ca-certificates build-essential zlib1g-dev libncurses5-dev libgdbm-dev libnss3-dev libssl-dev libsqlite3-dev libreadline-dev libffi-dev curl libbz2-dev && \
#    wget https://www.python.org/ftp/python/3.8.12/Python-3.8.12.tgz && \
#    tar -xf Python-3.8.12.tgz && \
#    cd Python-3.8.12 && \
#    ./configure --enable-optimizations && \
#    make -j 4 && \
#    make altinstall && \
#    cd .. && \
#    rm -rf Python-3.8.12 && rm Python-3.8.12.tgz && \
#    apt-get -y purge zlib1g-dev libncurses5-dev libgdbm-dev libnss3-dev libssl-dev libsqlite3-dev libreadline-dev libffi-dev libbz2-dev && \
#    apt-get clean && \
#    rm -rf /var/lib/apt/lists/*

# Ensure Python 3.8 is the default
RUN update-alternatives --install "/usr/bin/python" "python" "$(which python3)" 1

# Fix the value of PYTHONHASHSEED
# Note: this is needed when you use Python 3.3 or greater
ENV SPARK_VERSION=3.2.4 \
HADOOP_VERSION=3.2 \
SPARK_HOME=/opt/spark \
PYTHONHASHSEED=1


RUN wget --no-verbose -O apache-spark.tgz "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
&& mkdir -p /opt/spark \
&& tar -xf apache-spark.tgz -C /opt/spark --strip-components=1 \
&& rm apache-spark.tgz

# Update PATH
ENV PATH $SPARK_HOME/bin:$PATH


# Apache spark environment
FROM builder as apache-spark

WORKDIR /opt/spark

ENV SPARK_MASTER_PORT=7077 \
SPARK_MASTER_WEBUI_PORT=8080 \
SPARK_LOG_DIR=/opt/spark/logs \
SPARK_MASTER_LOG=/opt/spark/logs/spark-master.out \
SPARK_WORKER_LOG=/opt/spark/logs/spark-worker.out \
SPARK_WORKER_WEBUI_PORT=8080 \
SPARK_WORKER_PORT=7000 \
SPARK_MASTER="spark://spark-master:7077" \
SPARK_WORKLOAD="master"

EXPOSE 8080 7077 6066

RUN mkdir -p $SPARK_LOG_DIR && \
touch $SPARK_MASTER_LOG && \
touch $SPARK_WORKER_LOG && \
ln -sf /dev/stdout $SPARK_MASTER_LOG && \
ln -sf /dev/stdout $SPARK_WORKER_LOG

RUN pip3 install --upgrade pip setuptools wheel
COPY req.txt .
RUN pip3 install -r req.txt
#COPY spark-defaults.conf conf/

ENV \
    AZURE_STORAGE_VER=8.6.4 \
    HADOOP_AZURE_VER=3.2.1 \
    JETTY_VER=9.4.45.v20220203

COPY ./jars/*.jar $SPARK_HOME/jars/
# Set JARS env
ENV JARS=${SPARK_HOME}/jars/azure-storage-${AZURE_STORAGE_VER}.jar,${SPARK_HOME}/jars/hadoop-azure-${HADOOP_AZURE_VER}.jar,${SPARK_HOME}/jars/jetty-util-ajax-${JETTY_VER}.jar,${SPARK_HOME}/jars/jetty-util-${JETTY_VER}.jar

RUN echo "spark.jars ${JARS}" >> $SPARK_HOME/conf/spark-defaults.conf


COPY start-spark.sh /
CMD ["/bin/bash", "/start-spark.sh"]

