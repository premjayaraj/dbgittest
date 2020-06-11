# Databricks notebook source
# MAGIC %sh nc -zv mdbtest.mariadb.database.azure.com 3306
# MAGIC  

# COMMAND ----------

# MAGIC %sh nc -zv mysqldb-1.mysql.database.azure.com 3306

# COMMAND ----------

#spark.hadoop.javax.jdo.option.ConnectionURL jdbc:mariadb://mdbtest.mariadb.database.azure.com:3306/hive
#spark.hadoop.javax.jdo.option.ConnectionUserName testadmin@mymdbtest
#spark.hadoop.javax.jdo.option.ConnectionPassword Welcome@1
#spark.hadoop.javax.jdo.option.ConnectionDriverName org.mariadb.jdbc.Driver 
#spark.sql.hive.metastore.version 1.2.1
#spark.sql.hive.metastore.jars builtin
#datanucleus.autoCreateSchema true
#datanucleus.fixedDatastore false

# COMMAND ----------

# MAGIC %sql show databases;

# COMMAND ----------

# MAGIC 
# MAGIC %sql CREATE DATABASE testdb2;

# COMMAND ----------

# MAGIC %sql

# COMMAND ----------

# MAGIC %sql CREATE TABLE testdb2.checklists (prem INT);

# COMMAND ----------

# MAGIC %scala 
# MAGIC 
# MAGIC import com.typesafe.config.ConfigFactory
# MAGIC val path = ConfigFactory.load().getString("java.io.tmpdir")
# MAGIC println(s"\nHive JARs are downloaded to the path: $path \n")

# COMMAND ----------

# MAGIC %sh ls -l /local_disk0/tmp/ | grep -i hive

# COMMAND ----------

# MAGIC %sh mkdir -p /dbfs/prem/extMetastore/jars/1.2.2/

# COMMAND ----------

# MAGIC %sh cp -r /local_disk0/tmp/hive-v1_2-aa514646-b67b-4b31-8bc1-c3a7362ca9db/* /dbfs/prem/extMetastore/jars/1.2.2/
# MAGIC  

# COMMAND ----------

# MAGIC %sh ls /dbfs/prem/extMetastore/jars/1.2.2/ | grep hive

# COMMAND ----------

# MAGIC %sh ls -l /dbfs/prem/extMetastore/2.3.4/hive-v2_3-436ea2b4-e3e7-428a-8a77-a8df505dbb07 | grep hive

# COMMAND ----------

# MAGIC %sh cp -r /dbfs/prem/extMetastore/2.3.4/hive-v2_3-436ea2b4-e3e7-428a-8a77-a8df505dbb07/ /dbfs/prem/extMetastore/jars/

# COMMAND ----------

# MAGIC %sh ls /dbfs/prem/extMetastore/jars/3.1.1/ 

# COMMAND ----------

# MAGIC  %sh ls -la /dbfs/prem/extMetastore/jars/ | grep hive

# COMMAND ----------

# MAGIC  %sh mkdir -p /dbfs/prem/extMetastore/2.3.4/

# COMMAND ----------

# MAGIC %sql use default;

# COMMAND ----------

# MAGIC %sql show tables;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT requestParams.spark_version, COUNT(*)
# MAGIC FROM audit_logs
# MAGIC WHERE serviceName = "clusters" AND actionName = "create"
# MAGIC GROUP BY requestParams.spark_version

# COMMAND ----------

# MAGIC %sh
# MAGIC AWS_BUCKET_NAME = "<aws-bucket-name>"
# MAGIC MOUNT_NAME = "<mount-name>"
# MAGIC dbutils.fs.mount("s3a://%s" % AWS_BUCKET_NAME, "/mnt/%s" % MOUNT_NAME)
# MAGIC display(dbutils.fs.ls("/mnt/%s" % MOUNT_NAME))

# COMMAND ----------

# MAGIC %scala
# MAGIC val AwsBucketName = "prem-audit-log"
# MAGIC val MountName = "premaudit"
# MAGIC 
# MAGIC dbutils.fs.mount(s"s3a://$AwsBucketName", s"/mnt/$MountName")
# MAGIC display(dbutils.fs.ls(s"/mnt/$MountName"))

# COMMAND ----------

# MAGIC %sh ls -l /dbfs/mnt/premaudit/folder1

# COMMAND ----------

dbutils.fs.mount("s3a://prem-audit-log", "/mnt/audit")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT userIdentity.email, sourceIPAddress
# MAGIC FROM audit_logs
# MAGIC WHERE serviceName = "accounts" AND actionName LIKE "%login%"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT create,
# MAGIC FROM audit_logs
# MAGIC WHERE serviceName = "dbfs", and action = "create"

# COMMAND ----------

# MAGIC %scala
# MAGIC val df = sqlContext.read.json("s3a://prem-audit-log/*")
# MAGIC df.createOrReplaceTempView("audit_logs")

# COMMAND ----------

# MAGIC %sh pwd

# COMMAND ----------

# MAGIC %sh ls -l /dbfs

# COMMAND ----------

# MAGIC %sh ls -l /dbfs/mnt/premaudit

# COMMAND ----------

# MAGIC %sh ls -l /dbfs/mnt/premaudit/

# COMMAND ----------

# MAGIC %sh cat /dbfs/mnt/premaudit/_databricks_dev

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT useridentify.email
# MAGIC FROM audit_logs
# MAGIC WHERE serviceName = "dbfs", and actionName = "create"

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT timestamp,response, userIdentity.email
# MAGIC FROM audit_logs
# MAGIC WHERE requestParams.init_scripts like "%kernel_gateway_init%" 

# COMMAND ----------

# MAGIC %sql SELECT * FROM audit_logs 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT max(timestamp)
# MAGIC FROM audit_logs;

# COMMAND ----------

# MAGIC %sql select * from audit_logs where serviceName = "dbfs" and actionName = "create" and requestParams.path like "%dbfs:/init/%"

# COMMAND ----------

# MAGIC %sql select * from audit_logs where serviceName = "dbfs"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT distinct actionName 
# MAGIC FROM audit_logs

# COMMAND ----------

# MAGIC %sh echo "this is test" > /dbfs/databricks/init/prem_test_cli.sh

# COMMAND ----------

# MAGIC %sh rm /dbfs/databricks/init/prem*

# COMMAND ----------

# MAGIC %sh /dbfs/databricks/init/prem_test_cli.sh

# COMMAND ----------

dbutils.fs.put("dbfs:/databricks/init/prem_test_dbfs.sh" ,"""
#!/bin/bash

echo "hello" >> /hello.txt
""", True)

# COMMAND ----------

#write a file to DBFS using Python I/O APIs
with open("/dbfs/databricks/init/prem_test_python.sh", 'w') as f:
  f.write("Apache Spark is awesome!\n")
  f.write("End of example!")

# read the file
with open("/dbfs/databricks/init/prem_test_python.sh", "r") as f_read:
  for line in f_read:
    print (line)

# COMMAND ----------

dbutils.fs.rm("/dbfs/databricks/init/prem_test_python.sh")

# COMMAND ----------

# MAGIC %sh ls -l /dbfs/databricks/init/

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT distinct actionName 
# MAGIC FROM audit_logs

# COMMAND ----------

# MAGIC %sh ls -l /dbfs/mnt/premaudit/folder1

# COMMAND ----------

# MAGIC %sh echo -n dapi1de7b9b2a216c52312f7cfc1c4d389ef | openssl dgst -sha256

# COMMAND ----------

# MAGIC %sh sudo -H pip install awscli

# COMMAND ----------

# MAGIC %sql select * from audit_logs where serviceName = "dbfs" and requestParams.path like "%prem_test%"

# COMMAND ----------

# MAGIC %sh nc -zv database-1.cisil1o3lhzx.ap-northeast-2.rds.amazonaws.com 3306

# COMMAND ----------

#spark.hadoop.javax.jdo.option.ConnectionURL jdbc:mariadb://database-1.cisil1o3lhzx.ap-northeast-2.rds.amazonaws.com:3306/hive
#spark.hadoop.javax.jdo.option.ConnectionUserName admin
#spark.hadoop.javax.jdo.option.ConnectionPassword Welcome123
#spark.hadoop.javax.jdo.option.ConnectionDriverName org.mariadb.jdbc.Driver 
#spark.sql.hive.metastore.version 1.2.1
#spark.sql.hive.metastore.jars builtin
#datanucleus.autoCreateSchema true
#datanucleus.fixedDatastore false

# COMMAND ----------

