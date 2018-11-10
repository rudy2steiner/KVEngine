package com.alibabacloud.polar_race.engine.common;

public interface Lifecycle {
     default boolean isStarted(){
          return false;
     }
     void start() throws Exception;
     void close() throws Exception;

     /**
      *  启动
      * @throws Exception
      */
     void onStart() throws Exception;

     /**
      *  暂停
      */
     void onPause() throws Exception;

     /**
      *  恢复
      **/
     void onResume() throws Exception;


     /**
      *  停止
      */
     void onStop() throws Exception;
}
