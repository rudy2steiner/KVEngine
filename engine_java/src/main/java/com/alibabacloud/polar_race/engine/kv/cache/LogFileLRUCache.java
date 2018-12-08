package com.alibabacloud.polar_race.engine.kv.cache;


import com.alibabacloud.polar_race.engine.common.Service;
import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.alibabacloud.polar_race.engine.common.io.IOHandler;
import com.alibabacloud.polar_race.engine.common.utils.Bytes;
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
 * no cache put file
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
    private ExecutorService loadService;
//    private LogCacheMonitor logCacheMonitor;
    private IOHandlerLRUCache logHandlerCache;
    long segmentSizeMask=StoreConfig.SEGMENT_LOG_FILE_SIZE-1;
    long rightShift= Bytes.bitSpace(StoreConfig.SEGMENT_LOG_FILE_SIZE);
    int  maxRecordInSingleLog=StoreConfig.SEGMENT_LOG_FILE_SIZE/StoreConfig.VALUE_SIZE;
    int  maxRecordMask=maxRecordInSingleLog-1;
    int leftShift=Bytes.bitSpace(maxRecordInSingleLog);
    public LogFileLRUCache(LogFileService logFileService,IOHandlerLRUCache logHandlerCache,CacheController cacheController,ExecutorService loadService,LogBufferAllocator logBufferAllocator){
        this.logFileService=logFileService;
        this.logHandlerCache=logHandlerCache;
        this.sortedLogFiles=new TreeSet();
        this.cacheController=cacheController;//new KVCacheController(logFileService);
        this.maxCacheLog =cacheController.maxCacheLog();
        this.loadService =loadService;
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
                 throw  new EngineException(RetCodeEnum.CORRUPTION,"put error");
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
    public  void readValueIfCacheMiss(long expectedKey,int offset,ByteBuffer buffer) throws EngineException{
//        long fileId=sortedLogFiles.floor(offset);
//        long offsetInFile = offset - fileId;
        //cache miss,direct io
            IOHandler handler=null;
            try {
                int fileId=offset>>>leftShift;
                int segmentOffset=offset&maxRecordMask;
                long offsetInFile=segmentOffset*StoreConfig.LOG_ELEMENT_SIZE;
                handler = logHandlerCache.getHandler(fileId);//logFileService.ioHandler(filename + StoreConfig.LOG_FILE_SUFFIX,"r");////logHandlerCache.getHandler(fileId);//
                if(handler==null) throw new EngineException(RetCodeEnum.IO_ERROR,"io handler not found");
                buffer.limit(StoreConfig.VALUE_SIZE);
                long valueOffset=offsetInFile+StoreConfig.LOG_ELEMENT_LEAST_SIZE;
                if(handler.length()>=valueOffset+StoreConfig.VALUE_SIZE){
                    //skip to value offset, 对同一个文件的并发读
                    synchronized (handler) {
                        //handler.position(valueOffset);
                        handler.read(valueOffset,buffer);
                    }
                }else {
                    throw new EngineException(RetCodeEnum.INCOMPLETE,String.format("%d missing in file %d",expectedKey,fileId));
                }
                // notify cache miss
                //handler.closeFileChannel(false);
            }catch (Exception e){
                if(e instanceof EngineException)
                    throw  (EngineException) e;
               logger.info("direct io exception",e);
            }


    }




    @Override
    public void onStart() throws Exception {
        if(logFileService.allLogFiles().size()>0){
            sortedLogFiles.addAll(logHandlerCache.files());
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
        logger.info("put cache on stop!");
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
     * put cache buffer pool
     **/
    public BufferHolder getLogBufferHolder(){
//         BufferHolder holder= logBufferAllocator.allocateDirectLogCache();
//         if(holder==null) holder=logBufferAllocator.allocateHeapLogCache();
//         if(holder==null) {
//             throw new IllegalArgumentException("allocate put buffer failed");
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
//        concurrency=Math.min(concurrency,logs.expectedSize());
//        int initLoad=(int)(cacheController.cacheLogLoadFactor()*cacheController.maxCacheLog());
//        int logSize=logs.expectedSize();
//           initLoad=logSize>initLoad?initLoad:logSize;
//        if(concurrency>0) {
//            if(loadService==null)
//                loadService = Executors.newFixedThreadPool(concurrency);
//            for (int i=0;i<initLoad;i++){
//                loadService.submit(new LogLoadCacheTask(logReader,logs.get(i),cacheController,logCacheMonitor,this));
//            }
//        }

    }


    /**
     * load the put to cache
     **/
    @Deprecated
    public void loadCache(long logFileName){
        loadService.submit(new LogLoadCacheTask(logReader,logFileName,cacheController,null,this));
    }

}
