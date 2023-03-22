FROM ubuntu:20.04

#update ubuntu's packages
RUN apt update
RUN apt -y upgrade

#install wget
RUN apt-get update && \
    apt-get install -y --no-install-recommends wget

#install python and its libraries
RUN apt install -y python3-pip
RUN pip install pandas && pip install numpy
RUN pip install requests
RUN pip install Pillow
RUN pip install plotly
RUN pip install streamlit

RUN apt update
RUN apt -y upgrade

#install java
RUN DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends openjdk-11-jre
RUN DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends openjdk-11-jdk

#install spark
RUN wget https://downloads.apache.org/spark/spark-3.2.3/spark-3.2.3-bin-hadoop2.7.tgz
RUN tar -xvzf spark-3.2.3-bin-hadoop2.7.tgz 
RUN mv spark-3.2.3-bin-hadoop2.7 /usr/local/spark 
RUN rm -rf spark-3.2.3-bin-hadoop2.7.tgz

#set spark and python environment
ENV SPARK_HOME=/usr/local/spark
ENV PATH=$PATH:$SPARK_HOME/bin

ENV PYSPARK_PYTHON=/usr/bin/python3.8
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3.8

#install pyspark
RUN pip3 install pyspark

COPY process.py ./
COPY main.py ./
COPY app.py ./
#docker run -it --name ubuntest ubunspark /bin/bash