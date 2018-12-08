package com.alibabacloud.polar_race.engine.kv.index;

import com.alibabacloud.polar_race.engine.common.Service;
import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.kv.cache.IOHandlerLRUCache;
import com.alibabacloud.polar_race.engine.kv.file.LogFileService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
public class IndexLogReader extends Service {
    private final static Logger logger= LoggerFactory.getLogger(IndexLogReader.class);
    private String logDir;
    private LogFileService logFileService;
    private List<Long>  logFiles;
    private ExecutorService indexHashService;
    private boolean ownThreadPool=false;
    private IOHandlerLRUCache logHandlerCache;
    public IndexLogReader(String logDir, LogFileService logFileService, IOHandlerLRUCache logHandlerCache, ExecutorService executorService){
           this.logDir=logDir;
           this.logFileService=logFileService;
           this.logHandlerCache=logHandlerCache;
           this.indexHashService=executorService;
    }

    @Override
    public void onStart() throws Exception {
          this.logFiles=logHandlerCache.files();
          if(indexHashService==null) {
              this.indexHashService = Executors.newFixedThreadPool(StoreConfig.HASH_CONCURRENCY);
              ownThreadPool=true;
          }
    }

    @Override
    public void onStop() throws Exception {
         if(!indexHashService.isShutdown()&&ownThreadPool){
             if(!indexHashService.isTerminated()){
                 if(indexHashService.awaitTermination(1, TimeUnit.SECONDS)){
                     logger.info("tasks finished,and shutdown");
                 }else{
                     logger.info("tasks asyncClose timeout,and shutdown");
                     indexHashService.shutdownNow();
                 }
             }
         }
    }

    public void iterate(IndexVisitor visitor,int concurrency) throws Exception{
        int perThreadFiles=1;
        int mod=0;
        int logCount=logFiles.size();
        if(logCount==0) return;
        if(logCount>concurrency){
            perThreadFiles=logFiles.size()/concurrency;
            mod=logFiles.size()%concurrency;
        }else{
            concurrency=logFiles.size();
        }
        int start;
        int end=0;
        Runnable task;
        for(int i=0;i<concurrency;i++){
            start=end;
            if(i<mod){
                end+=1;
            }
             end+=perThreadFiles;
             task= new IndexWalLogReader(logFileService,logHandlerCache, logFiles,start,end,visitor);
             logger.info(String.format("add task,start %d,end %d",start,end));
             indexHashService.submit(task);
        }
        if(ownThreadPool){
            indexHashService.shutdown();
            if(indexHashService.awaitTermination(50, TimeUnit.SECONDS)){
                logger.info("tasks finished,and shutdown");
            }else{
                logger.info("tasks timeout,and shutdown");
                indexHashService.shutdownNow();
            }
        }
    }
}
