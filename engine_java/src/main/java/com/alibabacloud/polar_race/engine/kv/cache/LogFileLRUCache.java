package com.alibabacloud.polar_race.engine.kv.cache;


import com.alibabacloud.polar_race.engine.common.Service;
import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.alibabacloud.polar_race.engine.common.io.IOHandler;
import com.alibabacloud.polar_race.engine.kv.file.LogFileService;
import com.alibabacloud.polar_race.engine.kv.buffer.BufferHolder;
import com.alibabacloud.polar_race.engine.kv.buffer.LogBufferAllocator;
import com.alibabacloud.polar_race.engine.kv.wal.WalReader;
import com.google.common.cache.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.ByteBuffer;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

/***
 * no cache log file
 **/
public class LogFileLRUCache extends Service {
    private final static Logger logger= LoggerFactory.getLogger(LogFileLRUCache.class);
    private LogFileService logFileService;
    private TreeSet<Long>  sortedLogFiles;
    private LoadingCache<Long, BufferHolder> lru;
    private CacheController cacheController;
    private int maxCacheLog;
//    private LogBufferAllocator logBufferAllocator;
    private WalReader logReader;
    private ExecutorService loadServcie;
//    private LogCacheMonitor logCacheMonitor;
    public LogFileLRUCache(LogFileService logFileService,CacheController cacheController,ExecutorService loadService,LogBufferAllocator logBufferAllocator){
        this.logFileService=logFileService;
        this.sortedLogFiles=new TreeSet();
        this.cacheController=cacheController;//new KVCacheController(logFileService);
        this.maxCacheLog =cacheController.maxCacheLog();
        this.loadServcie=loadService;
//        this.logBufferAllocator=logBufferAllocator;
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
         if(slice.remaining()>StoreConfig.LOG_ELEMENT_LEAST_SIZE){
             short len=slice.getShort();
             long  actualKey=slice.getLong();
             if(actualKey!=expectedKey){
                 throw new IllegalArgumentException("read record error");
             }
             if(len!=StoreConfig.KEY_VALUE_SIZE) {
                 logger.info(String.format("file %d,offset %d,key %d,len %d ", fileId, offsetInFile, expectedKey, len));
                 throw  new EngineException(RetCodeEnum.CORRUPTION,"log error");
             }
             slice.limit(slice.position()+StoreConfig.VALUE_SIZE);
             buffer.put(slice);
         }else {
             throw new EngineException(RetCodeEnum.NOT_FOUND,String.format("%d missing in file %d",expectedKey,fileId));
         }
         holder.release();
    }


    /**
     * 根据key and offset 取value
     **/
    public  void readValueIfCacheMiss(long expectedKey,long offset,ByteBuffer buffer) throws EngineException{
        long fileId=sortedLogFiles.floor(offset);
        long offsetInFile = offset - fileId;
       //cache miss,direct io
            IOHandler handler=null;
            try {
                handler = logFileService.ioHandler(fileId + StoreConfig.LOG_FILE_SUFFIX);
                buffer.limit(StoreConfig.VALUE_SIZE);
                long valueOffset=offsetInFile+StoreConfig.LOG_ELEMENT_LEAST_SIZE;
                if(handler.length()>=valueOffset+StoreConfig.VALUE_SIZE){
                    //skip to value offset
                    handler.position(valueOffset);
                    handler.read(buffer);
                }else {
                    throw new EngineException(RetCodeEnum.INCOMPLETE,String.format("%d missing in file %d",expectedKey,fileId));
                }
                // notify cache miss
                handler.closeFileChannel(false);
            }catch (Exception e){
                if(e instanceof EngineException)
                    throw  (EngineException) e;
               logger.info("direct io exception",e);
            }


    }




    @Override
    public void onStart() throws Exception {
        if(logFileService.allLogFiles().size()>0){
            sortedLogFiles.addAll(logFileService.allLogFiles());
//            for()
//            this.lru= CacheBuilder.newBuilder()
//                    .maximumSize(maxCacheLog)
//                    .removalListener(new LogFileRemoveListener())
//                    .build(new CacheLoader<Long, BufferHolder>() {
//                        @Override
//                        public BufferHolder load(Long logId) throws Exception {
//                            BufferHolder holder=getLogBufferHolder();
//                            logReader.read(logId+StoreConfig.LOG_FILE_SUFFIX,holder.value(),cacheController.cacheLogSize());
//                            holder.value().flip();
//                             return holder;
//                        }
//                    });
//            logCacheMonitor=new LogCacheMonitor(logFileService,lru,this);
//            logCacheMonitor.start();
           // concurrentInitLoadCache();
        }
    }



    @Override
    public void onStop() throws Exception {
        //logBufferAllocator.close();
//        logCacheMonitor.close();
        logger.info("log cache on stop!");
    }

    public class LogFileRemoveListener implements RemovalListener<Long,BufferHolder> {
        AtomicInteger removeCounter=new AtomicInteger(0);
        @Override
        public void onRemoval(RemovalNotification<Long, BufferHolder> removalNotification) {
            if(removeCounter.incrementAndGet()%1000==0)
                 logger.info(String.format("cache miss %d total, lead remove %d",removeCounter.get(),removalNotification.getKey()));
            //  monitor record
//            try {
                // notify cache miss
//            BufferHolder holder=removalNotification.getValue();
//                if (holder.value().isDirect()) {
//                    logBufferAllocator.rebackDirectLogCache(holder);
//                } else {
//                    logBufferAllocator.rebackHeapLogCache(holder);
//                }
//            }catch (InterruptedException e){
//                logger.info("lru cache out, interrupted");
//            }
        }
    }


    /**
     * log cache buffer pool
     **/
    public BufferHolder getLogBufferHolder(){
//         BufferHolder holder= logBufferAllocator.allocateDirectLogCache();
//         if(holder==null) holder=logBufferAllocator.allocateHeapLogCache();
//         if(holder==null) {
//             throw new IllegalArgumentException("allocate log buffer failed");
//         }
         return null;
    }

    /**
     *
     **/
    @Deprecated
    public void concurrentInitLoadCache(){
//        int concurrency=cacheController.cacheLogInitLoadConcurrency();
//        List<Long> logs=logFileService.allLogFiles();
//        concurrency=Math.min(concurrency,logs.size());
//        int initLoad=(int)(cacheController.cacheLogLoadFactor()*cacheController.maxCacheLog());
//        int logSize=logs.size();
//           initLoad=logSize>initLoad?initLoad:logSize;
//        if(concurrency>0) {
//            if(loadServcie==null)
//                loadServcie = Executors.newFixedThreadPool(concurrency);
//            for (int i=0;i<initLoad;i++){
//                loadServcie.submit(new LogLoadCacheTask(logReader,logs.get(i),cacheController,logCacheMonitor,this));
//            }
//        }

    }


    /**
     * load the log to cache
     **/
    @Deprecated
    public void loadCache(long logFileName){
        loadServcie.submit(new LogLoadCacheTask(logReader,logFileName,cacheController,null,this));
    }

}
