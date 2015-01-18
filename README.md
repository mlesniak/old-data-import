# Introduction

Small example to write a file to HDFS.

### Exception: Caused by: org.apache.hadoop.ipc.RemoteException: Permission denied

To access the HDFS-filesystem of the cluster, the current user $USER has to have an account on the namenode and
write permissions to the directory. For the Cloudera-VM this means (for $USER == mlesniak)

    sudo adduser mlesniak
    sudo usermod -a -G cloudera mlesniak
    hadoop fs -chmod g+w /user/cloudera

