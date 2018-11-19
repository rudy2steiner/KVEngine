package com.alibabacloud.polar_race.engine.kv.cache;

import com.carrotsearch.hppc.LongIntHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class IndexCacheListener implements CacheListener<LongIntHashMap>{
    private final static Logger logger= LoggerFactory.getLogger(IndexCacheListener.class);
    Map<Integer, LongIntHashMap> lru;
    private AtomicInteger cachedCountDown;
    private CountDownLatch latch;
    public IndexCacheListener(Map<Integer, LongIntHashMap> lru, CountDownLatch latch, int bucketSize){
        this.lru=lru;
        this.latch=latch;
        this.cachedCountDown=new AtomicInteger(bucketSize);

    }
    @Override
    public void onRemove(long bucketId, LongIntHashMap map) {

    }

    @Override
    public void onMissCache(long id) {

    }

    @Override
    public void onCache(long bucketId, LongIntHashMap map) {
        lru.put((int)bucketId,map);
        if(cachedCountDown.decrementAndGet()==0){
            latch.countDown();
            logger.info("index cache completely");
        }
    }
}
