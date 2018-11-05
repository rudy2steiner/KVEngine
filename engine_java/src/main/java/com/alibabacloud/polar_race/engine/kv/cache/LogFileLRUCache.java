package com.alibabacloud.polar_race.engine.kv.cache;

import com.alibabacloud.polar_race.engine.common.Lifecycle;
import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.alibabacloud.polar_race.engine.kv.LogFileService;
import com.alibabacloud.polar_race.engine.kv.buffer.BufferHolder;
import com.alibabacloud.polar_race.engine.kv.buffer.LogBufferAllocator;
import com.alibabacloud.polar_race.engine.kv.wal.WalReader;
import com.google.common.cache.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class LogFileLRUCache implements Lifecycle {
    private final static Logger logger= LoggerFactory.getLogger(LogFileLRUCache.class);
    private AtomicBoolean started=new AtomicBoolean(false);
    private LogFileService logFileService;
    private TreeSet<Long>  sortedLogFiles;
    private LoadingCache<Long, BufferHolder> lru;
    private CacheController cacheController;
    private int maxCacheLog;
    private LogBufferAllocator logBufferAllocator;
    private WalReader logReader;
    public LogFileLRUCache(LogFileService logFileService){
        this.logFileService=logFileService;
        this.sortedLogFiles=new TreeSet();
        this.cacheController=new KVCacheController(logFileService);
        this.maxCacheLog =cacheController.maxCacheLog();
        this.logReader=new WalReader(logFileService);
    }


    /**
     * 根据key and offset 取value
     **/
    public  byte[] getValue(long expectedKey,long offset) throws ExecutionException,EngineException{
         long fileId=sortedLogFiles.ceiling(offset);
         BufferHolder holder=lru.get(fileId);
         holder.retain();
         int offsetInFile=(int)(offset-fileId);
         ByteBuffer  slice=holder.value().slice();
                     slice.position(offsetInFile);
         byte[] values=new byte[StoreConfig.VALUE_SIZE];
         if(slice.remaining()>StoreConfig.LOG_ELEMNT_LEAST_SIZE){
             slice.position(offsetInFile+2);
             long  actualKey=slice.getLong();
             if(actualKey!=expectedKey){
                 throw new IllegalArgumentException("read record error");
             }
             slice.get(values);
         }else {
             throw new EngineException(RetCodeEnum.NOT_FOUND,String.format("%d missing in file %d",expectedKey,fileId));
         }
         holder.release();
        return values;
    }


    @Override
    public boolean isStart() {
        return started.get();
    }

    @Override
    public void start() throws Exception {
        if(!isStart()){
            int maxDirectCacheLog=cacheController.maxDirectBuffer()/cacheController.cacheLogSize();
            int maxHeapCacheLog=cacheController.maxCacheLog()-maxDirectCacheLog+ StoreConfig.MAX_CONCURRENCY_PRODUCER_AND_CONSUMER;
            logger.info(String.format("max direct cache log file %d, heap %d",maxDirectCacheLog,maxHeapCacheLog));
            this.logBufferAllocator=new LogBufferAllocator(logFileService,maxDirectCacheLog,maxHeapCacheLog);

            sortedLogFiles.addAll(logFileService.allLogFiles());
            this.lru= CacheBuilder.newBuilder()
                    .maximumSize(maxCacheLog)
                    .removalListener(new LogFileRemoveListener())
                    .build(new CacheLoader<Long, BufferHolder>() {
                        @Override
                        public BufferHolder load(Long logId) throws Exception {
                            BufferHolder holder=getLogBufferHolder();
                            logReader.read(logId+StoreConfig.LOG_FILE_SUFFIX,holder.value(),cacheController.cacheLogSize());
                            holder.value().flip();
                             return holder;
                        }
                    });
            concurrentInitLoadCache();
            started.compareAndSet(false,true);
        }
    }



    @Override
    public void close() throws Exception {
              logBufferAllocator.close();
    }

    public class LogFileRemoveListener implements RemovalListener<Long,BufferHolder> {
        @Override
        public void onRemoval(RemovalNotification<Long, BufferHolder> removalNotification) {
            logger.info(String.format("remove %d",removalNotification.getKey()));
            BufferHolder holder=removalNotification.getValue();
            try {
                if (holder.value().isDirect()) {
                    logBufferAllocator.rebackDirect(holder);
                } else {
                    logBufferAllocator.rebackHeap(holder);
                }
            }catch (InterruptedException e){
                logger.info("lru cache out, interrupted");
            }
        }
    }


    public BufferHolder getLogBufferHolder(){
         BufferHolder holder= logBufferAllocator.allocatDirect();
         if(holder==null) holder=logBufferAllocator.allocateHeap();
         if(holder==null) throw new IllegalArgumentException("allocate log buffer failed");
         return holder;
    }

    public void concurrentInitLoadCache() throws InterruptedException{
        int concurrency=cacheController.cacheLogInitLoadConcurrency();
        List<Long> logs=logFileService.allLogFiles();
        concurrency=Math.min(concurrency,logs.size());
        int initLoad=(int)(cacheController.cacheLogLoadFactor()*cacheController.maxCacheLog());
        if(concurrency>0) {
            ExecutorService loadServcie = Executors.newFixedThreadPool(concurrency);
            for (int i=0;i<initLoad;i++){
                loadServcie.submit(new LoadLogFile(logReader,String.valueOf(logs.get(i))));
            }
            loadServcie.shutdown();
            if(loadServcie.awaitTermination(10, TimeUnit.SECONDS)){
                logger.info("load cache log finish");
            }else{
                logger.info("load cache log timeout,continue");
            }
        }

    }



    public class LoadLogFile implements Runnable{
        private WalReader reader;
        private String fileName;
         public LoadLogFile(WalReader reader, String fileName){
             this.reader=reader;
             this.fileName=fileName;
         }
        @Override
        public void run() {
             try {
                 BufferHolder holder=getLogBufferHolder();
                 reader.read(fileName,holder.value() , cacheController.cacheLogSize());
                 getLogBufferHolder().value().flip();
                 // cache
                 lru.put(Long.valueOf(fileName),holder);
             }catch (Exception  e){
                 logger.info("read log to cache failed",e);
             }
        }
    }
}
