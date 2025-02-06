FROM openjdk:8-jdk

LABEL maintainer="aminenafdou@gmail.com"
LABEL description="Simple hadoop cluster distributed as dockers containers"
LABEL codetype="java"
LABEL license="Hadoop: Apache License 2.0"

WORKDIR /opt

# Use Bash as the default shell with 'pipefail' to ensure pipeline failures are detected
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# Disable the installation of suggested and recommended packages in APT to minimize dependencies
RUN echo 'APT::Install-Suggests "0";' >> /etc/apt/apt.conf.d/10disableextras && \
    echo 'APT::Install-Recommends "0";' >> /etc/apt/apt.conf.d/10disableextras

# Minimize the verbosity of output during package configuration
ENV DEBCONF_TERSE=true

# Set the system PATH to include Hadoop binaries and other essential directories
ENV PATH=${JAVA_HOME}/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/opt/hadoop/bin

# Specify the directory where Hadoop logs will be stored
ENV HADOOP_LOG_DIR=/var/log/hadoop

# Define the configuration directory for Hadoop
ENV HADOOP_CONF_DIR=/etc/hadoop

# Set frontend for Debian package manager to noninteractive
ENV DEBIAN_FRONTEND=noninteractive

# Could be changed in the next version so maybe it's better to fix it here...
ARG HADOOP_URL=https://dlcdn.apache.org/hadoop/common/hadoop-3.4.1/hadoop-3.4.1.tar.gz

# Update APT and install additional packages (note: OpenJDK is provided by the base image)
RUN apt update -y && \
    apt install -y sudo python3-pip wget curl krb5-user

# Create a new group named 'hadoop' with the group ID (GID) set to 1000
RUN groupadd --gid 1000 hadoop

# Create a new user named 'hadoop' with the user ID (UID) set to 1000,
# assign the user to a group (GID 100), and set the home directory to /opt/hadoop
RUN useradd --uid 1000 hadoop --gid 100 --home /opt/hadoop

# Add a sudoers entry to allow the 'hadoop' user passwordless sudo access to all commands globally
RUN echo "hadoop ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers

# Add a sudoers entry for members of the 'hadoop' group, stored in a dedicated sudoers file
RUN echo "%hadoop ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers.d/hadoop

# Download and install Hadoop
RUN sudo rm -rf /opt/hadoop && \
    curl -LSs -o hadoop.tar.gz $HADOOP_URL && \
    tar zxf hadoop.tar.gz && rm hadoop.tar.gz && \
    mv hadoop* hadoop && \
    rm -rf /opt/hadoop/share/doc

# Create the necessary directories and configure permissions
RUN mkdir -p /etc/security/keytabs/hadoop && \
    mkdir -p /etc/hadoop && \
    mkdir -p /var/log/hadoop && \
    chown -R hadoop:hadoop /etc/security/keytabs && \
    chown -R hadoop:hadoop /opt && \
    chmod -R 770 /etc/security/keytabs && \
    chmod -R 1777 /etc/hadoop && \
    chmod -R 1777 /var/log/hadoop

# Add users hdfs, yarn, mapred to the hadoop group (which is a sudoer)
RUN useradd -d /opt/hadoop -u 1001 -g 1000 hdfs && \
    useradd -d /opt/hadoop -u 1002 -g 1000 yarn && \
    useradd -d /opt/hadoop -u 1003 -g 1000 mapred && \
    # User that will be used for tests...
    useradd -d /opt/hadoop -u 1004 -g 1000 tester

# Install debugging tools
RUN apt install -y net-tools bind9-utils dnsutils vim

WORKDIR /opt/hadoop
USER hadoop
