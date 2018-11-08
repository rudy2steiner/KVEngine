package com.alibabacloud.polar_race.engine.kv.cache;

import com.alibabacloud.polar_race.engine.common.Lifecycle;
import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.alibabacloud.polar_race.engine.kv.file.LogFileService;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
    private ExecutorService loadServcie;
    public LogFileLRUCache(LogFileService logFileService,CacheController cacheController,ExecutorService loadServcie,LogBufferAllocator logBufferAllocator){
        this.logFileService=logFileService;
        this.sortedLogFiles=new TreeSet();
        this.cacheController=cacheController;//new KVCacheController(logFileService);
        this.maxCacheLog =cacheController.maxCacheLog();
        this.loadServcie=loadServcie;
        this.logBufferAllocator=logBufferAllocator;
        this.logReader=new WalReader(logFileService);
    }


    /**
     * 根据key and offset 取value
     **/
    public  void readValue(long expectedKey,long offset,ByteBuffer buffer) throws ExecutionException,EngineException{
         long fileId=sortedLogFiles.floor(offset);
         BufferHolder holder=lru.get(fileId);
                      holder.retain();
         int offsetInFile=(int)(offset-fileId);
         ByteBuffer  slice=holder.value().slice();
                     slice.position(offsetInFile);
         if(slice.remaining()>StoreConfig.LOG_ELEMNT_LEAST_SIZE){
             short len=slice.getShort();
             long  actualKey=slice.getLong();
             if(actualKey!=expectedKey){
                 throw new IllegalArgumentException("read record error");
             }
             if(len!=StoreConfig.KEY_VALUE_SIZE) {
                 logger.info(String.format("file %d,offset %d,key %,len %d ", fileId, offsetInFile, expectedKey, len));
                 throw  new EngineException(RetCodeEnum.CORRUPTION,"log error");
             }
             slice.limit(slice.position()+StoreConfig.VALUE_SIZE);
             buffer.put(slice);
         }else {
             throw new EngineException(RetCodeEnum.NOT_FOUND,String.format("%d missing in file %d",expectedKey,fileId));
         }
         holder.release();
    }


    @Override
    public boolean isStart() {
        return started.get();
    }

    @Override
    public void start() throws Exception {
        if(!isStart()&&logFileService.allLogFiles().size()>0){

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
        if(isStart())
            logBufferAllocator.close();
    }

    public class LogFileRemoveListener implements RemovalListener<Long,BufferHolder> {
        AtomicInteger removeCounter=new AtomicInteger(0);
        @Override
        public void onRemoval(RemovalNotification<Long, BufferHolder> removalNotification) {
            if(removeCounter.incrementAndGet()%1000==0)
                 logger.info(String.format("remove %d",removalNotification.getKey()));
            BufferHolder holder=removalNotification.getValue();
            try {
                if (holder.value().isDirect()) {
                    logBufferAllocator.rebackDirectLogCache(holder);
                } else {
                    logBufferAllocator.rebackHeapLogCache(holder);
                }
            }catch (InterruptedException e){
                logger.info("lru cache out, interrupted");
            }
        }
    }


    public BufferHolder getLogBufferHolder(){
         BufferHolder holder= logBufferAllocator.allocateDirectLogCache();
         if(holder==null) holder=logBufferAllocator.allocateHeapLogCache();
         if(holder==null) throw new IllegalArgumentException("allocate log buffer failed");
         return holder;
    }

    public void concurrentInitLoadCache() throws InterruptedException{
        int concurrency=cacheController.cacheLogInitLoadConcurrency();
        List<Long> logs=logFileService.allLogFiles();
        concurrency=Math.min(concurrency,logs.size());
        int initLoad=(int)(cacheController.cacheLogInitLoadConcurrency()*cacheController.maxCacheLog());
        int logSize=logs.size();
           initLoad=logSize>initLoad?initLoad:logSize;
        if(concurrency>0) {
            if(loadServcie==null)
                loadServcie = Executors.newFixedThreadPool(concurrency);
            for (int i=0;i<initLoad;i++){
                loadServcie.submit(new LoadLogFile(logReader,logs.get(i)));
            }
        }

    }



    public class LoadLogFile implements Runnable{
        private WalReader reader;
        // without suffix
        private long fileName;
         public LoadLogFile(WalReader reader,long fileName){
             this.reader=reader;
             this.fileName=fileName;
         }
        @Override
        public void run() {
             try {
                 BufferHolder holder=getLogBufferHolder();
                 int cacheLogSize=cacheController.cacheLogSize();
                 reader.read(fileName+StoreConfig.LOG_FILE_SUFFIX,holder.value() , cacheLogSize);
                 holder.value().flip();
                 // cache
                 lru.put(fileName,holder);
             }catch (Exception  e){
                 logger.info("read log to cache failed",e);
             }
        }
    }
}
