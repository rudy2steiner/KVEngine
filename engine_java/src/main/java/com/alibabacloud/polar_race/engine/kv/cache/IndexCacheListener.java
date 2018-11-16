package com.alibabacloud.polar_race.engine.kv.cache;

import com.carrotsearch.hppc.LongLongHashMap;
import com.google.common.cache.LoadingCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class IndexCacheListener implements CacheListener<LongLongHashMap>{
    private final static Logger logger= LoggerFactory.getLogger(IndexCacheListener.class);
    LoadingCache<Integer, LongLongHashMap> lru;
    private AtomicInteger cachedCountDown;
    private CountDownLatch latch;
    public IndexCacheListener(LoadingCache<Integer, LongLongHashMap> lru, CountDownLatch latch, int bucketSize){
        this.lru=lru;
        this.latch=latch;
        this.cachedCountDown=new AtomicInteger(bucketSize);

    }
    @Override
    public void onRemove(long bucketId, LongLongHashMap map) {

    }

    @Override
    public void onMissCache(long id) {

    }

    @Override
    public void onCache(long bucketId, LongLongHashMap map) {
        lru.put((int)bucketId,map);
        if(cachedCountDown.decrementAndGet()==0){
            latch.countDown();
            logger.info("index cache completely");
        }
    }
}
