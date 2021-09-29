#!/usr/bin/env bash
ps -ef |grep video_info |awk '{print $2}' | xargs kill -9
