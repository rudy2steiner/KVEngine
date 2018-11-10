package com.alibabacloud.polar_race.engine.common;


import java.util.concurrent.atomic.AtomicBoolean;

/**
 * task service
 * 拥有生命周期
 *
 **/
public  abstract class Service implements Lifecycle {
    private AtomicBoolean started=new AtomicBoolean(false);
    private AtomicBoolean paused=new AtomicBoolean(false);



    /**
     * 服务启动
     **/
    public void start() throws Exception {
        if(!isStarted()&&started.compareAndSet(false,true)){
            onStart();
        }
    }

    /**
     * 服务暂停
     **/
    public void pause() throws Exception{
        if(isStarted()&&paused.compareAndSet(false,true)){
            onPause();
        }
    }

    /**
     * 服务恢复
     **/
    public void resume() throws Exception{
      if(isStarted()&&paused.compareAndSet(true,false)){
          onResume();
      }
    }


    /**
     * 服务停止
     **/
    public void stop() throws Exception{
        if(isStarted()&&started.compareAndSet(true,false)){
            onStop();
        }
    }

    @Override
    public void close() throws Exception {
         stop();
    }

    @Override
    public void onStart() throws Exception {

    }

    @Override
    public void onPause() throws Exception {

    }

    @Override
    public void onResume() throws Exception {

    }

    @Override
    public void onStop() throws Exception {

    }

    /**
     * 是否启动
     **/
    public boolean isStarted() {
        return started.get();
    }
}
