提取mysql(mariadb)中的耗时事务(基于python-mysql-replication)，后面可考虑使用容器部署


## 环境要求：

python 2.7

复制账号权限 grant select,replication slave,replication client on *.* to repl@''

mariadb需要将 binlog_annotate_row_events 关闭


## pypy部署（可提高性能，建议该方式部署和执行脚本）

yum -y install pypy-libs pypy pypy-devel

curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py

pypy get-pip.py

git clone https://github.com/lenovore/etl-time-consuming-transactions.git

cd etl-time-consuming-transactions

pypy -m pip install -r requirements.txt

#修改配置和设置binlog pos


#查看参数

pypy etl-trans.py -h

#启动

nohup /data/etl-time-consuming-transactions/start.sh 2>&1 >/dev/null &


## 保存到mysql的表结构

create table long_transaction (
  id int primary key auto_increment, db varchar (50), cost_second int, start_time datetime, end_time datetime, bin_file varchar (20), start_pos bigint, end_pos bigint, querys longtext
)

## 注意
1、从文件中获取pos，会出现kill掉进程后，文件内容为空的情况(不要使用kill -9), 启动时要观察下

## 参考

https://github.com/noplay/python-mysql-replication

https://github.com/yymysql/mysql-clickhouse-replication

https://github.com/long2ice/mysql2ch
