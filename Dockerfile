FROM ubuntu as BUILDIMAGE

# Env variables
ENV DOCKERIZE_VERSION v0.6.1
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64

# Installing missing dependencies
RUN apt-get -y update
RUN apt-get install python3-pip -y
RUN pip3 install pyspark
RUN DEBIAN_FRONTEND="noninteractive" apt-get -y install tzdata
RUN \
    apt-get update && \
    apt-get install -y openjdk-8-jre && \
    rm -rf /var/lib/apt/lists/*

COPY . .

# Run pyspark script
CMD python3 ./main.py