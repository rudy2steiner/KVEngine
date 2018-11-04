package com.alibabacloud.polar_race.engine.common;

public interface Lifecycle {
     default boolean isStart(){
          return false;
     }
     void start() throws Exception;
     void close() throws Exception;
}
