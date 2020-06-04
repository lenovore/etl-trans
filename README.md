提取mysql(mariadb)中的耗时事务(基于python-mysql-replication)，后面可考虑使用容器部署


环境要求：

python 2.7


pypy部署（可提高性能，建议该方式部署和执行脚本）

yum -y install pypy-libs pypy pypy-devel

curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py

pypy get-pip.py

pypy -m pip install -r requirements.txt


CPython部署

pip install -r requirements.txt


使用

pypy etl-trans.py -h

cd etl-time-consuming-transactions && nohup pypy etl-trans.py 2>&1 >/dev/null &

参考

https://github.com/noplay/python-mysql-replication

https://github.com/yymysql/mysql-clickhouse-replication

https://github.com/long2ice/mysql2ch
