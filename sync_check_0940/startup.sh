export PYTHON3_HOME=/home/hopson/apps/usr/webserver/dba/python3.6.0
export LD_LIBRARY_PATH=${PYTHON3_HOME}/lib
export SYNC_HOME=/home/hopson/apps/usr/webserver/sync_check_0940
nohup ${PYTHON3_HOME}/bin/python3 ${SYNC_HOME}/sync_check.py -conf ${SYNC_HOME}/sync_check.ini &>/tmp/sync_check2.log 2>&1 &
