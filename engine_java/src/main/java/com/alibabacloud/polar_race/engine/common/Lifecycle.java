package com.alibabacloud.polar_race.engine.common;

public interface Lifecycle {
     void start() throws Exception;
     void close() throws Exception;
}
