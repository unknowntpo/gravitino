#
# Copyright 2023 Datastrato Pvt Ltd.
# This software is licensed under the Apache License version 2.
#
# Apache Ranger Admin runtime Docker image
FROM debian:buster
LABEL maintainer="support@datastrato.com"

WORKDIR /root

RUN apt-get -q update && \
    apt-get install -y -q python python3 gcc mariadb-server vim curl wget openjdk-11-jdk git procps && \
    rm -rf /var/lib/apt/lists/*

ARG RANGER_VERSION=2.4.0
ENV RANGER_PASSWORD=rangerR0cks!
ARG MYSQL_CONNECTOR_PACKAGE_NAME
ARG LOG4JDBC_PACKAGE_NAME

COPY init-mysql.sql.template /tmp/
COPY start-ranger-services.sh /tmp/
RUN chmod +x /tmp/start-ranger-services.sh

ENV JAVA_HOME=/usr/local/jdk
RUN ARCH=$(uname -m) && \
    if [ "$ARCH" = "aarch64" ] || [ "$ARCH" = "arm64" ]; then \
    ln -s /usr/lib/jvm/java-11-openjdk-arm64 ${JAVA_HOME}; \
    else \
    ln -s /usr/lib/jvm/java-11-openjdk-amd64 ${JAVA_HOME}; \
    fi

# install ranger
ENV RANGER_HOME=/opt/ranger-admin
ADD packages/ranger-${RANGER_VERSION}-admin.tar.gz /opt/
RUN ln -s /opt/ranger-${RANGER_VERSION}-admin ${RANGER_HOME}

# Initialize Ranger envirioment
# We download these packages because ranger-admin required thiese packages, but it seems not included in tar file.
# And in ${RANGER_HOME}/setup.sh, it will use copy_db_connector() function to copy it to correct location.
COPY packages/${MYSQL_CONNECTOR_PACKAGE_NAME} /usr/share/java/mysql-connector-java.jar
RUN cp -r ${RANGER_HOME}/ews/webapp/WEB-INF/classes/conf.dist/ ${RANGER_HOME}/ews/webapp/WEB-INF/classes/conf && \
    mkdir ${RANGER_HOME}/ews/logs

EXPOSE 6080

ENTRYPOINT ["/bin/bash", "-c", "/tmp/start-ranger-services.sh"]
