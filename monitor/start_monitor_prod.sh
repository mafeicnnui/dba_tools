export PYTHON3_HOME=/home/hopson/apps/usr/webserver/python3.6.0
export MONITOR_HOME_BI=/home/hopson/apps/usr/webserver/dba/devops/monitor/prod
export LD_LIBRARY_PATH=${PYTHON3_HOME}/lib
export PATH=.:${PYTHON3_HOME}/bin:$PATH
nohup python3 ${MONITOR_HOME_BI}/mysql_monitor.py -conf ${MONITOR_HOME_BI}/mysql_monitor.ini  -mode alone  > ${MONITOR_HOME_BI}/mysql_monitor.log 2>&1 &
