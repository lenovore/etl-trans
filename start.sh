#!/bin/bash
# 直接执行导致进程老是被kill，后面可考虑使用docker，目前先这样简单处理了


sleep_sec=60
log=/data/etl-trans/etl.log
while :
do
    cd /data/etl-trans && /usr/bin/pypy etl-trans.py -r -m
    #/usr/bin/pypy /data/etl-trans/etl-trans.py -r -m
    echo $(date + "%F %T")" etl-trans停止，返回代码：$?" | tee -a $log
    echo "${sleep_sec}秒后再次启动" | tee -a $log
    sleep $sleep_sec
done
