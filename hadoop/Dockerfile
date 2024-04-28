# use Ubuntu latest version as the base image
FROM ubuntu:latest

# install OpenJDK 8 as dependency for hadoop instalation
RUN apt-get update
RUN apt-get install -y\
    ssh openjdk-8-jdk sudo curl gnupg procps python3 tar net-tools\
    openssh-server openssh-client


# clean up the package index and remove unnecessary files
RUN apt-get clean && rm -rf /var/lib/apt/lists/*

# set java environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

ARG HADOOP_VERSION=3.3.6
ARG HADOOP_URL=https://www.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz

ARG USER_NAME=hadoop
ARG GROUP_NAME=hadoop
ARG GID=5555
ARG UID=5555

ENV HADOOP_HOME=/usr/local/hadoop

# gain all root permissions for hadoop user 
RUN echo $USER_NAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USER_NAME 
RUN chmod 0440 /etc/sudoers.d/$USER_NAME 
RUN groupadd -g $GID $GROUP_NAME 
RUN useradd -m -s /bin/bash -u $UID -g $GID $USER_NAME

USER $USER_NAME 

# generate ssh-key
RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
RUN cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
RUN chmod 0600 ~/.ssh/authorized_keys

RUN sudo mkdir $HADOOP_HOME 

RUN sudo chown $USER_NAME:$GROUP_NAME -R $HADOOP_HOME

# download hadoop package
# download from website
RUN sudo curl -fsSL $HADOOP_URL -o /tmp/hadoop.tar.gz
# copy from local
# COPY --chown=$USER_NAME:$GROUP_NAME hadoop-3.3.6.tar.gz /tmp/hadoop.tar.gz

RUN sudo tar -xvzf /tmp/hadoop.tar.gz -C $HADOOP_HOME --strip-components 1 &&\
    mkdir $HADOOP_HOME/logs &&\
    rm /tmp/hadoop*

# create name node and datanode for single node
RUN sudo mkdir -p /home/hadoop/hdfs/datanode
RUN sudo mkdir /home/hadoop/hdfs/namenode
RUN sudo chown -R $USER_NAME:$GROUP_NAME /home/hadoop/hdfs 

ENV HADOOP_CONFIG=$HADOOP_HOME/etc/hadoop
ENV PATH=$HADOOP_HOME/sbin:$HADOOP_HOME/bin:$PATH 
ENV LD_LIBRARY_PATH=$HADOOP_HOME/lib/native:$LD_LIBRARY_PATH

# copy config file 
COPY --chown=$USER_NAME:$GROUP_NAME ./config/core-site.xml $HADOOP_CONFIG/
COPY --chown=$USER_NAME:$GROUP_NAME ./config/hdfs-site.xml $HADOOP_CONFIG/
COPY --chown=$USER_NAME:$GROUP_NAME ./config/mapred-site.xml $HADOOP_CONFIG/
COPY --chown=$USER_NAME:$GROUP_NAME ./config/hadoop-env.sh $HADOOP_CONFIG/
COPY --chown=$USER_NAME:$GROUP_NAME ./config/yarn-site.xml $HADOOP_CONFIG/


COPY entrypoint.sh /usr/local/sbin/entrypoint.sh 
RUN sudo chmod a+x /usr/local/sbin/entrypoint.sh

ENTRYPOINT [ "entrypoint.sh" ]
EXPOSE 9000 9870 50010 50020 50070 50075 50090