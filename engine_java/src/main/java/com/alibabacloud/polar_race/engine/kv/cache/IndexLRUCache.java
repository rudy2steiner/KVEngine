package com.alibabacloud.polar_race.engine.kv.cache;

import com.alibabacloud.polar_race.engine.common.Service;
import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.common.io.IOHandler;
import com.alibabacloud.polar_race.engine.kv.file.LogFileService;
import com.alibabacloud.polar_race.engine.kv.buffer.LogBufferAllocator;
import com.alibabacloud.polar_race.engine.kv.index.IndexHashAppender;
import com.alibabacloud.polar_race.engine.kv.index.IndexReader;
import com.google.common.cache.*;
import gnu.trove.map.hash.TLongLongHashMap;
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
    private LoadingCache<Integer, TLongLongHashMap> lru;
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
    public IndexLRUCache(CacheController cacheController , LogFileService indexFileService, ExecutorService indexLoadThreadPool, LogBufferAllocator bufferAllocator, CountDownLatch latch){
        this.cacheController=cacheController;
        this.maxCache=cacheController.maxCacheIndex();
        this.indexHandlerMap =new HashMap(128);
        this.indexFileService=indexFileService;
        this.indexReader=new IndexReader(indexFileService);
        this.byteBuffers=new ByteBuffer[cacheController.maxHashBucketSize()];
        this.maxConcurrencyLoad=cacheController.cacheIndexInitLoadConcurrency();
        this.indexLoadThreadPool=indexLoadThreadPool;
        this.bufferAllocator=bufferAllocator;
        this.loadComplete=latch;
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
                this.lru = CacheBuilder.newBuilder()
                        .maximumSize(maxCache)
                        .removalListener(new IndexRemoveListener())
                        .build(new IndexMapLoad(maxConcurrencyLoad));
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
    public long getOffset(long key){
        if(!this.isStarted()) throw new IllegalStateException("not started");
         int bucketId=IndexHashAppender.hash(key)%StoreConfig.HASH_BUCKET_SIZE;
         try {
             TLongLongHashMap longLongMap = lru.get(bucketId);
             if(longLongMap!=null) {
                return longLongMap.get(key);
                //to do read
             }else {
                 logger.info(String.format("cache miss %d int %d",key,bucketId));
             }
         }catch (ExecutionException e){
             logger.info("get exception ",e);
         }
         return -1;
    }

    public class IndexRemoveListener implements RemovalListener<Integer,TLongLongHashMap>{
        private int removeCount=0;
        @Override
        public void onRemoval(RemovalNotification<Integer, TLongLongHashMap> removalNotification) {
            if(++removeCount%1000==0)
                logger.info(String.format("%d cache miss total,remove %d",removeCount,removalNotification.getKey()));
        }
    }

    public class IndexMapLoad extends CacheLoader<Integer,TLongLongHashMap>{
        private Semaphore semaphore;
        private Object lock=new Object();
        public IndexMapLoad(int maxConcurrency){
               this.semaphore=new Semaphore(maxConcurrency);
        }
        @Override
        public TLongLongHashMap load(Integer bucketId) throws Exception {
            this.semaphore.acquire();
            ByteBuffer byteBuffer;
            int index;
            synchronized (lock) {
                while (true) {
                    index = bufferHolder.getAndIncrement()%byteBuffers.length;
                    if(byteBuffers[index] != null) {
                        byteBuffer=byteBuffers[index];
                        byteBuffers[index]=null;
                        break;
                    }
                }
            }
            TLongLongHashMap map= IndexReader.read(indexHandlerMap.get(bucketId),byteBuffer);
            // release buffer
            byteBuffers[index]=byteBuffer;
            this.semaphore.release();
            return map;
        }
    }


}
