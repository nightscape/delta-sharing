docker compose up -d --wait --wait-timeout 90
HADOOP_CONF_DIR="./configs/hadoop" KNOX_WEBHDFS_CONTEXT="/gateway/sandbox" KNOX_WEBHDFS_USERNAME="hadoop" KNOX_WEBHDFS_PASSWORD="hadoop-password" jbang --verbose --class-path ./configs/hadoop/core-site.xml DeltaSharingExample.java
