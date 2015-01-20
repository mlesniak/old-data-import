# Introduction

Small example to write a file to HDFS with parquet.

## Schema and class generation

The schema is defined in src/main/avro/table.avsc, generation with mvn generate-sources

## Impala

The statement to create an external Impala table is shown as a log message. It has to be executed just once. If you add
additional data files, do not forget to use refresh as in

    refresh <tableName>;

## Viewing

Use https://github.com/Parquet/parquet-mr/tree/master/parquet-tools to show the compressed parquet file.

## Problems and solutions

**Exception: Caused by: org.apache.hadoop.ipc.RemoteException: Permission denied**

To access the HDFS-filesystem of the cluster, the current user $USER has to have an account on the namenode and
write permissions to the directory. For the Cloudera-VM this means (for $USER == mlesniak)

    sudo adduser mlesniak
    sudo usermod -a -G cloudera mlesniak
    hadoop fs -chmod g+w /user/cloudera

