# Introduction

Small example to write a file to HDFS.

## Schema and class generation

The schema is defined in src/main/avro/table.avsc, generation with mvn generate-sources

## Problems and solutions

**Exception: Caused by: org.apache.hadoop.ipc.RemoteException: Permission denied**

To access the HDFS-filesystem of the cluster, the current user $USER has to have an account on the namenode and
write permissions to the directory. For the Cloudera-VM this means (for $USER == mlesniak)

    sudo adduser mlesniak
    sudo usermod -a -G cloudera mlesniak
    hadoop fs -chmod g+w /user/cloudera

