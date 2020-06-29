提取mysql(mariadb)中的耗时事务(基于python-mysql-replication)，后面可考虑使用容器部署


## 环境要求：

python 2.7

复制账号权限 grant select,replication slave,replication client on *.* to repl@''

mariadb需要将 binlog_annotate_row_events 关闭


## pypy部署（可提高性能，建议该方式部署和执行脚本）

yum -y install pypy-libs pypy pypy-devel

curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py

pypy get-pip.py

git clone https://github.com/lenovore/etl-trans.git

cd etl-trans

pypy -m pip install -r requirements.txt

#修改配置和设置binlog pos


#查看参数

pypy etl-trans.py -h

#启动

nohup /etl-trans/start.sh 2>&1 >/dev/null &


## 保存到mysql的表结构

create table `long_transaction` (
  `id` int(11) not null auto_increment,
  `db` varchar(50) default null,
  `cost_second` int(11) default null,
  `start_time` datetime default null,
  `end_time` datetime default null,
  `bin_file` varchar(20) default null,
  `start_pos` bigint(20) default null,
  `end_pos` bigint(20) default null,
  `querys` longtext default null,
  `create_time` datetime default current_timestamp(),
  `host` varchar(50),
  primary key (`id`)
) engine=innodb


## 注意
1、从文件中获取pos，会出现kill掉进程后，文件内容为空的情况(不要使用kill -9), 所以最好使用redis来存取pos

## 参考

https://github.com/noplay/python-mysql-replication

https://github.com/yymysql/mysql-clickhouse-replication

https://github.com/long2ice/mysql2ch
