package com.alibabacloud.polar_race.engine.kv.event;

import com.alibabacloud.polar_race.engine.common.Service;
import com.alibabacloud.polar_race.engine.common.io.CloseFileTask;
import com.alibabacloud.polar_race.engine.common.io.CloseHandler;
import com.alibabacloud.polar_race.engine.common.io.IOHandler;
import com.alibabacloud.polar_race.engine.common.thread.NamedThreadFactory;
import com.alibabacloud.polar_race.engine.kv.file.LogFileService;
import com.alibabacloud.polar_race.engine.kv.file.LogFileServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class TaskBus extends Service implements CloseHandler {
    private final static Logger logger= LoggerFactory.getLogger(TaskBus.class);
    private AtomicInteger submitCount=new AtomicInteger(0);
    private ExecutorService executorService;
    private int concurrency;
    private BlockingQueue<Runnable> taskQueue;
    public TaskBus(int concurrency){
        this.concurrency=concurrency;
        this.taskQueue=new LinkedBlockingQueue();

    }

    public void onStart(){
        this.executorService= new ThreadPoolExecutor(concurrency,concurrency,60,TimeUnit.SECONDS,taskQueue,new NamedThreadFactory("flush page"));

    }

    /**
     * 提交可执行任务
     **/
    public void submit(Runnable task){
        if(submitCount.incrementAndGet()%100000==0){
            logger.info(String.format("task queued %d",taskQueue.size()));
        }
        executorService.submit(task);
    }
    public void onStop(){
        executorService.shutdown();
        try {
            logger.info("task queued "+taskQueue.size());
            if(executorService.awaitTermination(10, TimeUnit.SECONDS)){
                logger.info("close file finished");
            }else{
                logger.info("close file timeout 10s ");
            }
        }catch (InterruptedException e){
            logger.info("await task bus finish interrupted",e);
        }
    }

    @Override
    public void close(IOHandler handler) {
        submit(new CloseFileTask(handler));
    }
}
