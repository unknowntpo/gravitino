#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
set -ex

trino_dir="$(dirname "${BASH_SOURCE-$0}")"
trino_dir="$(cd "${trino_dir}">/dev/null; pwd)"
gravitino_home="$(cd "${trino_dir}/../../..">/dev/null; pwd)"

# Prepare download packages
mkdir -p "${trino_dir}/packages"

cd ${gravitino_home}
${gravitino_home}/gradlew clean assembleTrinoConnector -x test
cp -r "${gravitino_home}/distribution/gravitino-trino-connector" "${trino_dir}/packages/gravitino-trino-connector"

MYSQL_VERSION="8.0.27"
PG_VERSION="42.7.0"
MYSQL_JAVA_CONNECTOR_URL="https://repo1.maven.org/maven2/mysql/mysql-connector-java/${MYSQL_VERSION}/mysql-connector-java-${MYSQL_VERSION}.jar"
PG_JAVA_CONNECTOR_URL="https://jdbc.postgresql.org/download/postgresql-${PG_VERSION}.jar"
RANGER_TRINO_PLUGIN_PACKAGE_NAME="ranger-2.4.0-trino-plugin.tar.gz"
# RANGER_TRINO_PLUGIN_PACKAGE_URL="https://github.com/unknowntpo/apache-ranger/releases/download/v2.4.41/${RANGER_TRINO_PLUGIN_PACKAGE_NAME}"
RANGER_TRINO_PLUGIN_PACKAGE_URL="https://github.com/unknowntpo/apache-ranger/releases/download/ranger-trino-plugin-ranger2.4.0-trino-435/${RANGER_TRINO_PLUGIN_PACKAGE_NAME}"
# Download MySQL jdbc driver if it does not exist.
if [ ! -f "${trino_dir}/packages/gravitino-trino-connector/mysql-connector-java-${MYSQL_VERSION}.jar" ]; then
  cd "${trino_dir}/packages/gravitino-trino-connector/" && curl -O "${MYSQL_JAVA_CONNECTOR_URL}" && cd -
fi

# Download PostgreSQL jdbc driver if it does not exist.
if [ ! -f "${trino_dir}/packages/gravitino-trino-connector/postgresql-{PG_VERSION}.jar" ]; then
  cd "${trino_dir}/packages/gravitino-trino-connector/" && curl -O "$PG_JAVA_CONNECTOR_URL" && cd -
fi

echo "xxx Here"
echo "trino dir: ${trino_dir}"

echo "RANGER_TRINO_PLUGIN_PACKAGE_NAME: ${RANGER_TRINO_PLUGIN_PACKAGE_NAME}"

# Download Ranger trino plugin if it does not exist.
if [ ! -f "${trino_dir}/packages/${RANGER_TRINO_PLUGIN_PACKAGE_NAME}" ]; then
  curl -L -s -o "${trino_dir}/packages/${RANGER_TRINO_PLUGIN_PACKAGE_NAME}" ${RANGER_TRINO_PLUGIN_PACKAGE_URL}
fi

echo "xxx Here done"


mkdir -p "${trino_dir}/packages/trino"
cp -r -p "${trino_dir}/conf" "${trino_dir}/packages/trino/conf"
