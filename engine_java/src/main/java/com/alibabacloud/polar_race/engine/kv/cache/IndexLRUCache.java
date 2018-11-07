package com.alibabacloud.polar_race.engine.kv.cache;

import com.alibabacloud.polar_race.collection.LongLongMap;
import com.alibabacloud.polar_race.engine.common.Lifecycle;
import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.common.io.IOHandler;
import com.alibabacloud.polar_race.engine.kv.LogFileService;
import com.alibabacloud.polar_race.engine.kv.buffer.BufferSizeAware;
import com.alibabacloud.polar_race.engine.kv.buffer.LogBufferAllocator;
import com.alibabacloud.polar_race.engine.kv.index.IndexHashAppender;
import com.alibabacloud.polar_race.engine.kv.index.IndexReader;
import com.google.common.cache.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class IndexLRUCache implements Lifecycle {
    private final static Logger logger= LoggerFactory.getLogger(IndexLRUCache.class);
    private LoadingCache<Integer, LongLongMap> lru;
    private Map<Integer, IOHandler> indexHandlerMap;
    private IndexReader indexReader;
    private LogFileService indexFileService;
    private volatile  ByteBuffer[]  byteBuffers;
    private int maxConcurrencyLoad=0;
    private AtomicBoolean started=new AtomicBoolean(false);
    private AtomicInteger bufferHolder=new AtomicInteger(0);
    private int maxCache;
    private CacheController cacheController;
    private ExecutorService indexLoadThreadPool;
    private LogBufferAllocator bufferAllocator;
    public IndexLRUCache(CacheController cacheController ,LogFileService indexFileService,ExecutorService indexLoadThreadPool,LogBufferAllocator bufferAllocator){
        this.cacheController=cacheController;
        this.maxCache=cacheController.maxCacheIndex();
        this.indexHandlerMap =new HashMap(128);
        this.indexFileService=indexFileService;
        this.indexReader=new IndexReader();
        this.byteBuffers=new ByteBuffer[cacheController.maxHashBucketSize()];
        this.maxConcurrencyLoad=cacheController.cacheIndexInitLoadConcurrency();
        this.indexLoadThreadPool=indexLoadThreadPool;
        this.bufferAllocator=bufferAllocator;
    }

    @Override
    public boolean isStart() {
        return started.get();
    }

    @Override
    public void start() throws Exception {
        if(!isStart()) {
            List<Long> indexFiles = indexFileService.allFiles(StoreConfig.LOG_INDEX_FILE_SUFFIX);
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
                indexReader.concurrentLoadIndex(indexLoadThreadPool, maxCache, Arrays.asList(byteBuffers).subList(0,maxCache), initCacheIndexHandler(), new IndexCacheListener(lru));
                started.compareAndSet(false, true);
            }
        }

    }

    /**
     *   遍历所有的key
     **/
    public void iterateKey() throws Exception{
        List<Long> indexFiles = indexFileService.allFiles(StoreConfig.LOG_INDEX_FILE_SUFFIX);
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
        int initLoadSize=(int)(StoreConfig.HASH_BUCKET_SIZE*StoreConfig.HASH_BUCKET_LOAD_FACTOR);
        return new ArrayList<>(indexHandlerMap.values()).subList(0,initLoadSize);
    }

    @Override
    public void close() throws Exception {
      if(isStart()){
          for(ByteBuffer buffer:byteBuffers){
              LogBufferAllocator.release(buffer);
          }
      }
    }

    /**
     * @param key
     * @return  fileId for the key or -1
     */
    public long getOffset(long key){
         int bucketId=IndexHashAppender.hash(key)%StoreConfig.HASH_BUCKET_SIZE;
         try {
             LongLongMap longLongMap = lru.get(bucketId);
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



    public class IndexRemoveListener implements RemovalListener<Integer,LongLongMap>{
        private int removeCount=0;
        @Override
        public void onRemoval(RemovalNotification<Integer, LongLongMap> removalNotification) {
            if(removeCount++%100==0)
                logger.info(String.format("remove %d",removalNotification.getKey()));
        }
    }

    public class IndexMapLoad extends CacheLoader<Integer,LongLongMap>{
        private Semaphore semaphore;
        private Object lock=new Object();
        public IndexMapLoad(int maxConcurrency){
               this.semaphore=new Semaphore(maxConcurrency);
        }
        @Override
        public LongLongMap load(Integer bucketId) throws Exception {
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
            LongLongMap map= IndexReader.read(indexHandlerMap.get(bucketId),byteBuffer);
            // release buffer
            byteBuffers[index]=byteBuffer;
            this.semaphore.release();
            return map;
        }
    }


}
