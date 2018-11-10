package com.alibabacloud.polar_race.engine.kv.event;

import java.util.concurrent.*;

public class TaskBus {

    private ExecutorService executorService;
    private int concurrency;
    public TaskBus(int concurrency){
        this.concurrency=concurrency;

    }

    public void start(){
        this.executorService= new ThreadPoolExecutor(concurrency,concurrency,60,TimeUnit.SECONDS,new LinkedBlockingQueue<>());
    }

    public void submit(Runnable task){
        executorService.submit(task);
    }

    public void close(){
        executorService.shutdown();
    }


}
