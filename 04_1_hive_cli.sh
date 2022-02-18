#-------Batch Mode 批处理模式-----------
#-e
$HIVE_HOME/bin/hive -e 'show databases'

#-f
cd ~
#编辑一个sql文件 里面写上合法正确的sql语句
vim hive.sql
show databases;
#执行 从客户端所在机器的本地磁盘加载文件
$HIVE_HOME/bin/hive -f /root/hive.sql
#也可以从其他文件系统加载sql文件执行
$HIVE_HOME/bin/hive -f hdfs://<namenode>:<port>/hive-script.sql
$HIVE_HOME/bin/hive -f s3://mys3bucket/s3-script.sql

#-i 进入交互模式之前运行初始化脚本
$HIVE_HOME/bin/hive -i /home/my/hive-init.sql

#使用静默模式将数据从查询中转储到文件中
$HIVE_HOME/bin/hive -S -e 'select * from itheima.student' > a.txt

#----------启动服务-------------------
#--hiveconf
$HIVE_HOME/bin/hive --hiveconf hive.root.logger=DEBUG,console

#--service
$HIVE_HOME/bin/hive --service metastore
$HIVE_HOME/bin/hive --service hiveserver2


#---------交互式模式运行
/export/server/hive/bin/hive
hive> show databases;
OK
default
itcast
itheima
Time taken: 0.028 seconds, Fetched: 3 row(s)
hive> use itcast;
OK
Time taken: 0.027 seconds
hive> exit;


#启用hive动态分区，需要在hive会话中设置两个参数：
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
