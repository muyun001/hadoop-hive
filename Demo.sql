show databases;

use gmall;
show tables;

create database if not exists Test;
use Test;
show tables;

create table if not exists t_students
(
    id   int,
    name varchar(255)
);
insert into table t_students
values (1, "zhangsan");
select *
from t_students;

create table if not exists t_user
(
    id   int,
    name varchar(255),
    age  int
);

create table t_user_1
(
    id   int,
    name varchar(255),
    age  int
)
    row format delimited
        fields terminated by ",";
select *
from t_user_1;

create table t_archer
(
    id           int comment "ID",
    name         string comment "英雄名称",
    hp_max       int comment "最大生命",
    mp_max       int comment "最大法力",
    attack_max   int comment "最高物攻",
    defense_max  int comment "最大物防",
    attack_range string comment "攻击范围",
    role_main    string comment "主要定位",
    role_assist  string comment "次要定位"
) comment "王者荣耀射手信息"
    row format delimited
        fields terminated by "\t";

-- load data local inpath "" into table table_name;
load data local inpath "/opt/module/hive-3.1.2/datas/archer.txt" into table t_archer;
select *
from t_archer;

create table if not exists t_hot_hero_skin_price
(
    id         int,
    name       string,
    win_rate   int,
    skin_price map<string,int>
) row format delimited
    fields terminated by ","
    collection items terminated by "-"
    map keys terminated by "-";

load data local inpath "/opt/module/hive-3.1.2/datas/hot_hero_skin_price.txt" into table t_hot_hero_skin_price;
select *
from t_hot_hero_skin_price;

drop table t_archer;

-- 创建外部表
create external table t_archer
(
    id           int comment "ID",
    name         string comment "英雄名称",
    hp_max       int comment "最大生命",
    mp_max       int comment "最大法力",
    attack_max   int comment "最高物攻",
    defense_max  int comment "最大物防",
    attack_range string comment "攻击范围",
    role_main    string comment "主要定位",
    role_assist  string comment "次要定位"
) comment "王者荣耀射手信息"
    row format delimited
        fields terminated by "\t";

drop table test.t_archer;
select *
from test.t_archer;

describe formatted test.t_archer;

drop table t_all_hero_part;
create external table t_all_hero_part
(
    id           int,
    name         string,
    hp_max       int,
    mp_max       int,
    attack_max   int,
    defense_max  int,
    attack_range string,
    role_main    string,
    role_assist  string

) partitioned by (role string)
    row format delimited
        fields terminated by "\t";

load data local inpath "/opt/module/hive-3.1.2/datas/archer.txt" into table t_all_hero_part partition (role = "archer");
load data local inpath "/opt/module/hive-3.1.2/datas/assassin.txt" into table t_all_hero_part partition (role = "cike");
load data local inpath "/opt/module/hive-3.1.2/datas/mage.txt" into table t_all_hero_part partition (role = "fashi");
load data local inpath "/opt/module/hive-3.1.2/datas/support.txt" into table t_all_hero_part partition (role = "fuzhu");
load data local inpath "/opt/module/hive-3.1.2/datas/tank.txt" into table t_all_hero_part partition (role = "tanke");
select *
from t_all_hero_part
where role = "cike";
-- select count(*) from t_all_hero where role_main="archer" and hp_max >6000;
select count(*)
from t_all_hero_part
where role = "sheshou"
  and hp_max > 6000;

CREATE TABLE t_usa_covid19_bucket
(
    count_date string,
    county     string,
    state      string,
    fips       int,
    cases      int,
    deaths     int
)
    clustered by (state) into 5 buckets;

drop table if exists t_usa_covid19;
CREATE TABLE t_usa_covid19
(
    count_date string,
    county     string,
    state      string,
    fips       int,
    cases      int,
    deaths     int
)
    row format delimited fields terminated by ",";

select *
from t_usa_covid19;

insert into t_usa_covid19_bucket
select *
from t_usa_covid19;
select *
from t_usa_covid19_bucket
where state = "Alabama";

drop table if exists t_students;
create table t_students
(
    id      int,
    name    string,
    gender  string,
    age     int,
    subject string
) row format delimited
    fields terminated by ",";

--Hive中事务表的创建使用
--1、开启事务配置（可以使用set设置当前session生效 也可以配置在hive-site.xml中）
set hive.support.concurrency = true; --Hive是否支持并发
set hive.enforce.bucketing = true; --从Hive2.0开始不再需要  是否开启分桶功能
set hive.exec.dynamic.partition.mode = nonstrict; --动态分区模式  非严格
set hive.txn.manager = org.apache.hadoop.hive.ql.lockmgr.DbTxnManager; --
set hive.compactor.initiator.on = true; --是否在Metastore实例上运行启动压缩合并
set hive.compactor.worker.threads = 1;
--在此metastore实例上运行多少个压缩程序工作线程。
-- 创建事务表
create table emp
(
    id     int,
    name   string,
    salary int
) stored as orc tblproperties ("transactional" = "true");
--事务表 insert  -->delta文件
INSERT INTO emp
VALUES (1, 'Jerry', 5000),
       (2, 'Tom', 8000),
       (3, 'Kate', 6000);
select *
from emp;

select *
from t_usa_covid19;
-- 创建视图
create view v_usa_covid19 as
select count_date, county, state, cases, deaths
from t_usa_covid19
limit 10;
show views;
select *
from v_usa_covid19;
-- 物化视图
set hive.support.concurrency = true; --Hive是否支持并发
set hive.enforce.bucketing = true; --从Hive2.0开始不再需要  是否开启分桶功能
set hive.exec.dynamic.partition.mode = nonstrict; --动态分区模式  非严格
set hive.txn.manager = org.apache.hadoop.hive.ql.lockmgr.DbTxnManager; --
set hive.compactor.initiator.on = true; --是否在Metastore实例上运行启动线程和清理线程
set hive.compactor.worker.threads = 1; --在此metastore实例上运行多少个压缩程序工作线程。

drop table if exists t_student_tran;
create table t_student_tran
(
    id      int,
    name    string,
    gender  string,
    age     int,
    subject string
) clustered by (id) into 2 buckets stored as orc tblproperties ("transactional" = "true");

insert overwrite table t_student_tran
select id, name, gender, age, subject
from t_students;

select *
from t_student_tran;

-- 创建物化视图
create materialized view student_trans_agg
as
select subject, count(*) as subject_cnt
from t_student_tran
group by subject;

show materialized views;

explain
select subject, count(*) as subject_cnt
from t_student_tran
group by subject;

use Test;
describe database test;
describe t_student_tran;
describe formatted t_student_tran;

alter table t_all_hero_part
    add partition (role = "qita");

alter table t_all_hero_part
    partition (role = "qita") rename to partition (role = "other");
describe formatted t_all_hero_part;
show partitions t_all_hero_part;
alter table t_all_hero_part
    partition (role = "archer") rename to partition (role = "sheshou");
select *
from t_all_hero_part
where role = "sheshou";

alter table t_all_hero_part
    drop partition (role = "other");
msck table t_all_hero_part;

-----MSCK 修复分区---------------
--Step1：创建分区表
create table t_all_hero_part_msck
(
    id           int,
    name         string,
    hp_max       int,
    mp_max       int,
    attack_max   int,
    defense_max  int,
    attack_range string,
    role_main    string,
    role_assist  string
) partitioned by (role string)
    row format delimited
        fields terminated by "\t";

show partitions t_all_hero_part_msck;
select *
from t_all_hero_part_msck;
msck repair table t_all_hero_part_msck add partitions;
-- metastore check 修复分区

-----------------------------------
--建表student_local 用于演示从本地加载数据
create table student_local
(
    num  int,
    name string,
    sex  string,
    age  int,
    dept string
) row format delimited fields terminated by ',';
--建表student_HDFS  用于演示从HDFS加载数据
create external table student_HDFS
(
    num  int,
    name string,
    sex  string,
    age  int,
    dept string
) row format delimited fields terminated by ',';
--建表student_HDFS_p 用于演示从HDFS加载数据到分区表
create table student_HDFS_p
(
    num  int,
    name string,
    sex  string,
    age  int,
    dept string
) partitioned by (country string) row format delimited fields terminated by ',';

-- 从本地加载数据
load data local inpath "/opt/module/hive-3.1.2/datas/students.txt" into table student_local;
-- 从HDFS加载数据

drop table if exists t_student;
create table t_student
(
    id      int,
    name    string,
    gender  string,
    age     int,
    subject string
) row format delimited fields terminated by ",";

--当前库下已有一张表student
select *
from t_student;
--创建两张新表
create table student_insert1
(
    id int
);
create table student_insert2
(
    name string
);

-- 多重插入：一次扫描，多次插入
from t_student
insert
overwrite
table
student_insert1
select id
insert
overwrite
table
student_insert2
select name;

-- 动态分区
--1、首先设置动态分区模式为非严格模式 默认已经开启了动态分区功能
set hive.exec.dynamic.partition = true;
set hive.exec.dynamic.partition.mode = nonstrict;

create table student_partition
(
    Sno   int,
    Sname string,
    Sex   string,
    Sage  int
) partitioned by (subject string);

insert into table student_partition partition (subject)
select id, name, gender, age, subject
from t_student;

-- 将表中数据导出到hdfs
insert overwrite directory "/tmp/export/student"
select *
from t_student;

-- 员工表
create table if not exists t_employee
(
    user_id  int,
    username string,
    dept_id  int
)
    row format delimited fields terminated by ' '
        lines terminated by '\n';

-- 部门表
create table if not exists t_dept
(
    dept_id   int,
    dept_name string
)
    row format delimited fields terminated by ' '
        lines terminated by '\n';

create table if not exists t_salary
(
    userid  int,
    dept_id int,
    salarys double
)
    row format delimited fields terminated by ' '
        lines terminated by '\n';

load data local inpath "/opt/module/hive-3.1.2/datas/employee.txt" into table t_employee;
load data local inpath "/opt/module/hive-3.1.2/datas/department.txt" into table t_dept;
load data local inpath "/opt/module/hive-3.1.2/datas/salary.txt" into table t_salary;

-- join
select e.username, e.dept_id, d.dept_name, d.dept_id
from t_employee e
         join t_dept d on e.dept_id = d.dept_id;

-- error
select e.username, e.dept_id, d.dept_name, d.dept_id
from t_employee e
         join t_dept d on e.dept_id <= d.dept_id;
select e.username, e.dept_id, d.dept_name, d.dept_id
from t_employee e
         join t_dept d on e.dept_id = d.dept_id or d.dept_id = 1;
-- 三表join
select e.username, d.dept_name, s.salarys
from t_employee e
         join t_dept d on e.dept_id = d.dept_id
         join t_salary s on e.user_id = s.userid

-- 行转列
create table person_info
(
    name          string,
    constellation string,
    blood_type    string
) row format delimited fields terminated by "\t";

load data local inpath "/opt/module/hive-3.1.2/datas/person_info.txt" into table person_info;

select *
from person_info;

select t1.c_b
from (
         select name,
                concat_ws(",", constellation, blood_type) c_b
         from person_info
     ) t1
group by t1.c_b;

select t1.c_b,
       concat_ws("|", collect_set(t1.name))
from (
         select name,
                concat_ws(",", constellation, blood_type) c_b
         from person_info
     ) t1
group by t1.c_b;
--射手座,A 大海|凤姐
--白羊座,A 孙悟空|猪八戒
--白羊座,B 宋宋|苍老师

-- 一行转多行
create table movie_info
(
    movie    string,
    category string
)
    row format delimited fields terminated by "\t";

load data local inpath "/opt/module/hive-3.1.2/datas/movie_info.txt" into table movie_info;


select movie,
       category_name
from movie_info
         lateral view explode(split(category, ",")) movie_info_tmp as category_name;

-- 窗口函数
create table bussiness
(
    name      string,
    orderdate string,
    cost      string
) row format delimited fields terminated by ",";

load data local inpath "/opt/module/hive-3.1.2/datas/bussiness.txt" into table bussiness;

-- 查询在 2017 年 4 月份购买过的顾客及总人数
select
       name,
       count(*)
        over()
from bussiness
where orderdate like '%2017-04%'  -- substring(orderdate,1,7) = '2017-04'
group by name;

-- 查询顾客的购买明细及月购买总额
select name,orderdate,cost,sum(cost) over(partition by month(orderdate))
from bussiness;


---建表并且加载数据
create table website_pv_info(
   cookieid string,
   createtime string,   --day
   pv int
) row format delimited
fields terminated by ',';

create table website_url_info (
    cookieid string,
    createtime string,  --访问时间
    url string       --访问页面
) row format delimited
fields terminated by ',';


load data local inpath '/opt/module/hive-3.1.2/datas/website_pv_info.txt' into table website_pv_info;
load data local inpath '/opt/module/hive-3.1.2/datas/website_url_info.txt' into table website_url_info;

select * from website_pv_info;
select * from website_url_info;

select cookieid,sum(pv) as total_pv from website_pv_info group by cookieid;
select cookieid,createtime,pv, sum(pv) over() as total_pv from website_pv_info;
select cookieid,createtime,pv, sum(pv) over(partition by cookieid) as total_pv from website_pv_info;
select cookieid,createtime,pv, sum(pv) over(partition by cookieid order by createtime) as total_pv from website_pv_info;
select cookieid,createtime,pv, sum(pv) over(partition by cookieid order by createtime rows between unbounded preceding and current row ) as total_pv from website_pv_info;
select cookieid,createtime,pv, sum(pv) over(partition by cookieid order by createtime rows between 3 preceding and current row ) as total_pv from website_pv_info;
select cookieid,createtime,pv, sum(pv) over(partition by cookieid order by createtime rows between 3 preceding and 1 following ) as total_pv from website_pv_info;
select cookieid,createtime,pv, sum(pv) over(partition by cookieid order by createtime rows between 3 preceding and unbounded following ) as total_pv from website_pv_info;
select cookieid,createtime,pv, sum(pv) over(partition by cookieid order by createtime rows between unbounded preceding and unbounded following ) as total_pv from website_pv_info;

select cookieid,createtime,pv,
       -- rank: 在每个分组中，为每行分配一个从1开始的序列号，考虑重复，挤占后续位置；
       rank() over (partition by cookieid order by pv desc ) as rn1,
       -- dense_rank: 在每个分组中，为每行分配一个从1开始的序列号，考虑重复，不挤占后续位置；
        dense_rank() over (partition by cookieid order by pv desc ) as rn2,
       -- row_number: 在每个分组中，为每行分配一个从1开始的唯一序列号，递增，不考虑重复；
       row_number() over  (partition by cookieid order by pv desc ) as rn3
from website_pv_info;

-- -需求：找出每个用户访问pv最多的Top3 重复并列的不考虑
select * from (
              select cookieid,createtime,pv,
       row_number() over (partition by cookieid order by pv desc ) as row_number
from website_pv_info
                  ) t1
where t1.row_number < 4;


-- ntile
-- -需求：统计每个用户pv数最多的前3分之1。
-- --理解：将数据根据cookieid分 根据pv倒序排序 排序之后分为3个部分 取第一部分
select cookieid, createtime, pv,
       ntile(3) over (partition by cookieid order by pv desc) as part
from website_pv_info;

select * from
(select cookieid, createtime, pv,
       ntile(3) over (partition by cookieid order by pv desc) as part
from website_pv_info) t1
where t1.part=2;

select * from t_student;

select * from t_student
distribute by rand() limit 2;

select * from t_student
tablesample ( 4 rows );
SELECT * FROM t_student TABLESAMPLE(50 PERCENT);


SELECT * FROM t_student TABLESAMPLE(1k);


select * from t_usa_covid19_bucket;
select state, count(*) as count from t_usa_covid19_bucket group by state;
desc extended t_usa_covid19_bucket;
---bucket table抽样--根据整行数据进行抽样
SELECT * FROM t_usa_covid19_bucket TABLESAMPLE(BUCKET 5 OUT OF 5 on rand());
SELECT * FROM t_usa_covid19_bucket TABLESAMPLE(BUCKET 1 OUT OF 5 ON state);