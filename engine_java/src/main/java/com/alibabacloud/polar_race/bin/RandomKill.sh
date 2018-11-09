#!/usr/bin/env bash
  key="benchmark"
  ps -ef|grep $key|grep java
  kill -9 $(ps -ef|grep $key|grep java|awk '{print $2}')
