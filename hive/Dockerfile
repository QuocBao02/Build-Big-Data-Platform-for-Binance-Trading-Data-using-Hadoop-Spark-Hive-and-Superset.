FROM quocbao/hadoop


ARG HIVE_VERSION=3.1.2
ARG USER_NAME=hadoop 
ARG GROUP_NAME=hadoop
ARG UID=5555
ARG GID=5555

USER hadoop
ENV HADOOP_HOME=/usr/local/hadoop 
ENV PATH=$PATH:$HADOOP_HOME/bin

ENV HIVE_HOME=/usr/local/hive
ARG HIVE_URL=https://archive.apache.org/dist/hive/hive-${HIVE_VERSION}/apache-hive-${HIVE_VERSION}-bin.tar.gz

USER ${USER_NAME}

# make hive home
RUN sudo mkdir -p $HIVE_HOME
RUN sudo chown $USER_NAME:$GROUP_NAME -R $HIVE_HOME 

# install hive
# from web 
# sudo curl -fsSL $HIVE_URL -o /tmp/hive.tar.gz 

# from local
COPY --chown=$USER_NAME:$GROUP_NAME apache-hive-3.1.2-bin.tar.gz /tmp/hive.tar.gz
RUN sudo tar -xvzf /tmp/hive.tar.gz -C $HIVE_HOME --strip-components 1
RUN sudo rm -rf /tmp/hive* 

# USER hadoop
# add mysql connector for hive 
# COPY  mysql-connector-java-8.3.0.jar $HIVE_HOME/lib
# COPY --chown=$USER_NAME:$GROUP_NAME mysql-metadata-storage-0.12.0.jar $HIVE_HOME/lib
RUN sudo wget https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.23/mysql-connector-java-8.0.23.jar -P $HIVE_HOME/lib


# set hive config and environment
ENV PATH=$PATH:$HIVE_HOME/bin 
ENV HIVE_CONF_DIR=$HIVE_HOME/conf

COPY --chown=$USER_NAME:$GROUP_NAME ./config/hive-site.xml $HIVE_CONF_DIR/

# ENV HIVE_METASTORE_JDBC_URL=jdbc:mysql://mysql-container:3306/metastore_db


COPY entrypoint.sh /entrypoint.sh 
RUN sudo chmod a+x /entrypoint.sh

ENTRYPOINT [ "/entrypoint.sh" ]

# expose hive port
EXPOSE 10000 10002

