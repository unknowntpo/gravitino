#!/bin/bash
#
# Copyright 2023 Datastrato Pvt Ltd.
# This software is licensed under the Apache License version 2.
#

# install Ranger hive plugin
if [[ -n "${RANGER_HIVE_REPOSITORY_NAME}" && -n "${RANGER_SERVER_URL}" ]]; then
  # If Hive enable Ranger plugin need requires zookeeper
  echo "Starting zookeeper..."
  #sed -i -r 's|#(log4j.appender.ROLLINGFILE.MaxBackupIndex.*)|\1|g' ${ZK_HOME}/conf/log4j.properties
  mv ${ZK_HOME}/conf/zoo_sample.cfg ${ZK_HOME}/conf/zoo.cfg
  #sed -i -r 's|#autopurge|autopurge|g' ${ZK_HOME}/conf/zoo.cfg
  sed -i "s|/tmp/zookeeper|${ZK_HOME}/data|g" ${ZK_HOME}/conf/zoo.cfg
  ${ZK_HOME}/bin/zkServer.sh start-foreground > /dev/null 2>&1 &

  cd ${RANGER_HIVE_PLUGIN_HOME}
  sed -i "s|POLICY_MGR_URL=|POLICY_MGR_URL=${RANGER_SERVER_URL}|g" install.properties
  sed -i "s|REPOSITORY_NAME=|REPOSITORY_NAME=${RANGER_HIVE_REPOSITORY_NAME}|g" install.properties
  echo "XAAUDIT.SUMMARY.ENABLE=true" >> install.properties
  sed -i "s|COMPONENT_INSTALL_DIR_NAME=|COMPONENT_INSTALL_DIR_NAME=${HIVE_HOME}|g" install.properties
  ${RANGER_HIVE_PLUGIN_HOME}/enable-hive-plugin.sh

  # Reduce poll policy interval in the ranger plugin configuration
  sed -i '/<name>ranger.plugin.hive.policy.pollIntervalMs<\/name>/{n;s/<value>30000<\/value>/<value>1000<\/value>/}' ${HIVE_HOME}/conf/ranger-hive-security.xml
fi

# install Ranger hdfs plugin
if [[ -n "${RANGER_HDFS_REPOSITORY_NAME}" && -n "${RANGER_SERVER_URL}" ]]; then
  cd ${RANGER_HDFS_PLUGIN_HOME}
  sed -i "s|POLICY_MGR_URL=|POLICY_MGR_URL=${RANGER_SERVER_URL}|g" install.properties
  sed -i "s|REPOSITORY_NAME=|REPOSITORY_NAME=${RANGER_HDFS_REPOSITORY_NAME}|g" install.properties
  echo "XAAUDIT.SUMMARY.ENABLE=true" >> install.properties
  sed -i "s|COMPONENT_INSTALL_DIR_NAME=|COMPONENT_INSTALL_DIR_NAME=${HADOOP_HOME}|g" install.properties
  ${RANGER_HDFS_PLUGIN_HOME}/enable-hdfs-plugin.sh

  # Reduce poll policy interval in the ranger plugin configuration
  sed -i '/<name>ranger.plugin.hive.policy.pollIntervalMs<\/name>/{n;s/<value>30000<\/value>/<value>1000<\/value>/}' ${HIVE_HOME}/conf/ranger-hdfs-security.xml
fi

# update hadoop config use hostname
sed -i "s/HOST_NAME/$(hostname)/g" ${HADOOP_CONF_DIR}/core-site.xml
sed -i "s/HOST_NAME/$(hostname)/g" ${HADOOP_CONF_DIR}/hdfs-site.xml
sed -i "s/HOST_NAME/$(hostname)/g" ${HIVE_HOME}/conf/hive-site.xml

# start hdfs
echo "Starting HDFS..."
echo "Format NameNode..."
${HADOOP_HOME}/bin/hdfs namenode -format -nonInteractive

echo "Starting NameNode..."
${HADOOP_HOME}/sbin/hadoop-daemon.sh start namenode

echo "Starting DataNode..."
${HADOOP_HOME}/sbin/hadoop-daemon.sh start datanode

# start mysql and create databases/users for hive
echo "Starting MySQL..."
chown -R mysql:mysql /var/lib/mysql
usermod -d /var/lib/mysql/ mysql
service mysql start

echo """
  CREATE USER 'hive'@'localhost' IDENTIFIED BY 'hive';
  GRANT ALL PRIVILEGES on *.* to 'hive'@'localhost' WITH GRANT OPTION;
  GRANT ALL on hive.* to 'hive'@'localhost' IDENTIFIED BY 'hive';
  CREATE USER 'iceberg'@'*' IDENTIFIED BY 'iceberg';
  GRANT ALL PRIVILEGES on *.* to 'iceberg'@'%' identified by 'iceberg' with grant option;
  FLUSH PRIVILEGES;
  CREATE DATABASE hive;
""" | mysql --user=root --password=${MYSQL_PWD}

# start hive
echo "Starting Hive..."
${HIVE_HOME}/bin/schematool -initSchema -dbType mysql
${HIVE_HOME}/bin/hive --service hiveserver2 > /dev/null 2>&1 &
${HIVE_HOME}/bin/hive --service metastore > /dev/null 2>&1 &

echo "Hive started successfully."

# persist the container
tail -f /dev/null
