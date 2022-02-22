#!/usr/bin/env bash
export WORKDIR='/home/hopson/apps/usr/webserver/dba/script'
export PYTHONUNBUFFERED="1"
export PYTHONPATH=${WORKDIR}
export PYTHON3_HOME=/usr/local/python3.6
export LD_LIBRARY_PATH=${PYTHON3_HOME}/lib
echo "Starting projects_server_stats task..."
 ${PYTHON3_HOME}/bin/python3 -u ${WORKDIR}/projects_server_stats_linux.py 
