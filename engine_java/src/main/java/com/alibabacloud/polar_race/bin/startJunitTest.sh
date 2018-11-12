#!/usr/bin/env bash
  key="benchmark"
  ps -ef|grep $key|grep java
  $(ps -ef|grep $key|grep java|awk '{print $2}') >/sys/fs/cgroup/memory/test/cgroup.procs



