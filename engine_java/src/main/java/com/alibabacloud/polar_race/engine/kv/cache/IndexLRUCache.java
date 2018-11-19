package com.alibabacloud.polar_race.engine.kv.cache;

import com.alibabacloud.polar_race.engine.common.Service;
import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.alibabacloud.polar_race.engine.common.io.IOHandler;
import com.alibabacloud.polar_race.engine.kv.file.LogFileService;
import com.alibabacloud.polar_race.engine.kv.buffer.LogBufferAllocator;
import com.alibabacloud.polar_race.engine.kv.index.IndexHashAppender;
import com.alibabacloud.polar_race.engine.kv.index.IndexReader;
import com.carrotsearch.hppc.LongIntHashMap;
import com.carrotsearch.hppc.LongIntMap;
import com.carrotsearch.hppc.LongLongHashMap;
import com.google.common.cache.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

public class IndexLRUCache extends Service {
    private final static Logger logger= LoggerFactory.getLogger(IndexLRUCache.class);
    private Map<Integer, LongIntHashMap> lru;
    private Map<Integer, IOHandler> indexHandlerMap;
    private IndexReader indexReader;
    private LogFileService indexFileService;
    private volatile  ByteBuffer[]  byteBuffers;
    private int maxConcurrencyLoad=0;
    private AtomicInteger bufferHolder=new AtomicInteger(0);
    private int maxCache;
    private CacheController cacheController;
    private ExecutorService indexLoadThreadPool;
    private LogBufferAllocator bufferAllocator;
    private CountDownLatch loadComplete;
    private int n;
    private int defaultValue=-1;
    public IndexLRUCache(CacheController cacheController , LogFileService indexFileService, ExecutorService indexLoadThreadPool, LogBufferAllocator bufferAllocator, CountDownLatch latch){
        this.cacheController=cacheController;
        this.maxCache=cacheController.maxCacheIndex();
        this.indexHandlerMap =new HashMap(128);
        this.lru=new HashMap<>(cacheController.maxHashBucketSize()*2);
        this.indexFileService=indexFileService;
        this.indexReader=new IndexReader(indexFileService);
        this.byteBuffers=new ByteBuffer[cacheController.maxHashBucketSize()];
        this.maxConcurrencyLoad=cacheController.cacheIndexInitLoadConcurrency();
        this.indexLoadThreadPool=indexLoadThreadPool;
        this.bufferAllocator=bufferAllocator;
        this.loadComplete=latch;
        this.n=cacheController.maxHashBucketSize()-1;// ensure
    }



    @Override
    public void onStart() throws Exception {
            List<Long> indexFiles = indexFileService.allSortedFiles(StoreConfig.LOG_INDEX_FILE_SUFFIX);
            if(indexFiles.size()>0) {
                for (Long fid : indexFiles) {
                    indexHandlerMap.put(fid.intValue(), indexFileService.ioHandler(fid + StoreConfig.LOG_INDEX_FILE_SUFFIX));
                }
                for (int i = 0; i < maxConcurrencyLoad; i++) {
                    byteBuffers[i] = bufferAllocator.allocate(cacheController.cacheIndexReadBufferSize(),true);
                }
                indexReader.concurrentLoadIndex(indexLoadThreadPool, maxConcurrencyLoad, Arrays.asList(byteBuffers).subList(0,maxConcurrencyLoad), initCacheIndexHandler(), new IndexCacheListener(lru,loadComplete,cacheController.maxHashBucketSize()));
            }
    }

    /**
     *   遍历所有的key
     **/
    public void iterateKey() throws Exception{
        List<Long> indexFiles = indexFileService.allSortedFiles(StoreConfig.LOG_INDEX_FILE_SUFFIX);
        if(indexFiles.size()>0) {
            for (Long fid : indexFiles) {
                indexHandlerMap.put(fid.intValue(), indexFileService.ioHandler(fid + StoreConfig.LOG_INDEX_FILE_SUFFIX));
            }
            for (int i = 0; i < maxConcurrencyLoad; i++) {
                byteBuffers[i] = bufferAllocator.allocate(cacheController.cacheIndexReadBufferSize(), true);
            }
            indexReader.concurrentLoadIndex(indexLoadThreadPool, byteBuffers.length, Arrays.asList(byteBuffers),new ArrayList<>(indexHandlerMap.values()),null);
        }
    }

    public List<IOHandler> initCacheIndexHandler(){
        int initLoadSize=(int)(StoreConfig.HASH_BUCKET_SIZE*cacheController.cacheIndexLoadFactor());
        return new ArrayList<>(indexHandlerMap.values()).subList(0,initLoadSize);
    }

    @Override
    public void onStop() throws Exception {
          for(ByteBuffer buffer:byteBuffers){
              LogBufferAllocator.release(buffer);
          }
          logger.info("index cache on stop!");

    }

    /**
     * @param key
     * @return  fileId for the key or -1
     */
    public int getOffset(long key) throws EngineException{
        if(!this.isStarted()) throw new EngineException(RetCodeEnum.CORRUPTION,"not started");
         int bucketId=IndexHashAppender.hash(key)&n;
         LongIntMap longLongMap = lru.get(bucketId);
             if(longLongMap!=null) {
                return longLongMap.getOrDefault(key,defaultValue);
                //to do read
             }else {
                 logger.info(String.format("cache miss %d int %d",key,bucketId));
             }
         return -1;
    }



}
