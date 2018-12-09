package com.alibabacloud.polar_race.engine.kv.index;

import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.common.Service;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.io.CloseHandler;
import com.alibabacloud.polar_race.engine.kv.buffer.LogBufferAllocator;
import com.alibabacloud.polar_race.engine.kv.cache.CacheController;
import com.alibabacloud.polar_race.engine.kv.cache.IOHandlerLRUCache;
import com.alibabacloud.polar_race.engine.kv.cache.IndexLRUCache;
import com.alibabacloud.polar_race.engine.kv.file.LogFileService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * kv index service
 *
 **/
public class KVIndexService extends Service implements IndexService {
    private final static Logger logger= LoggerFactory.getLogger(KVIndexService.class);
    private final static long BOOTSTRAP_TIMEOUT=5000;// 5S
    private String indexDir;
    private String walDir;
    private CacheController cacheController;
    private CloseHandler closeHandler;
    private IOHandlerLRUCache logHandlerCache;
    private LogFileService logFileService;
    private LogFileService indexFileService;
    private LogBufferAllocator logBufferAllocator;
    private IndexHashAppender hashIndexAppender;
    private IndexLogReader indexLogReader;
    private  ExecutorService commonExecutorService;
    private IndexLRUCache indexLRUCache;
    public KVIndexService(String indexDir, String walDir, CacheController cacheController, CloseHandler closeHandler, LogBufferAllocator logBufferAllocator,
                          LogFileService logFileService, LogFileService indexFileService , IOHandlerLRUCache logHandlerCache, ExecutorService commonExecutorService){
        this.indexDir=indexDir;
        this.walDir=walDir;
        this.cacheController=cacheController;
        this.closeHandler= closeHandler;
        this.logFileService=logFileService;
        this.indexFileService=indexFileService;
        this.logBufferAllocator=logBufferAllocator;
        this.commonExecutorService=commonExecutorService;
        this.logHandlerCache=logHandlerCache;
    }
    @Override
    public void onStart() throws Exception {
        CountDownLatch partitionIndexLatch=new CountDownLatch(1);
        startPartition(partitionIndexLatch);
        // to do timeout
        if(!partitionIndexLatch.await(BOOTSTRAP_TIMEOUT, TimeUnit.MILLISECONDS)){
           logger.info("hash index timeout ");
        }
        // hash finish and load into memory
        CountDownLatch indexLoadComplete=new CountDownLatch(1);
        loadIndex(indexLoadComplete);
        // wait load
        if(!indexLoadComplete.await(BOOTSTRAP_TIMEOUT,TimeUnit.MILLISECONDS)){
            logger.info("load index timeout ");
        }else {
            logger.info("load index done! ");
        }
    }

    @Override
    public void onStop() throws Exception {
        indexLRUCache.stop();
    }

    /**/
    public void startPartition(CountDownLatch startLatch) throws Exception{
        long start=System.currentTimeMillis();
        hashIndexAppender=new IndexHashAppender(indexDir,cacheController.maxHashBucketSize(),cacheController.hashBucketWriteCacheSize(),closeHandler);
        hashIndexAppender.start();
        indexLogReader=new IndexLogReader(walDir,logFileService, logHandlerCache,commonExecutorService);
        indexLogReader.start();
        logHandlerCache.start(); // cache io handler
        indexLogReader.iterate(new IndexVisitor() {
            private AtomicInteger concurrency=new AtomicInteger(cacheController.maxHashBucketSize());
            @Override
            public void visit(ByteBuffer buffer) throws Exception {
                try {
                    hashIndexAppender.append(buffer);
                }catch (Exception e){
                    logger.info("hash appender exception",e);
                    onException();
                    throw e;
                }
            }
            @Override
            public void onFinish() throws Exception{
                if(concurrency.decrementAndGet()==0) {
                    hashIndexAppender.close();
                    indexLogReader.close();
                    startLatch.countDown();
                    logger.info(String.format("hash task  finish, time %d ms",System.currentTimeMillis()-start));
                }
            }
            @Override
            public void onException()  {
                startLatch.countDown();
            }
        },cacheController.maxHashBucketSize());
    }
    @Override
    public int getOffset(long key) throws EngineException {
        return indexLRUCache.getOffset(key);
    }

    @Override
    public void range(long lower, long upper, AbstractVisitor iterator) {

    }

    public void loadIndex(CountDownLatch loadLatch) throws Exception {
        indexLRUCache=new IndexLRUCache(cacheController,indexFileService, commonExecutorService,logBufferAllocator,loadLatch);
        indexLRUCache.start();
    }
}
