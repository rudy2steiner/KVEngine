package com.alibabacloud.polar_race.engine.kv.cache;

import com.google.common.cache.LoadingCache;
import gnu.trove.map.hash.TLongLongHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class IndexCacheListener implements CacheListener<TLongLongHashMap>{
    private final static Logger logger= LoggerFactory.getLogger(IndexCacheListener.class);
    LoadingCache<Integer, TLongLongHashMap> lru;
    private AtomicInteger cachedCountDown;
    private CountDownLatch latch;
    public IndexCacheListener(LoadingCache<Integer, TLongLongHashMap> lru, CountDownLatch latch,int bucketSize){
        this.lru=lru;
        this.latch=latch;
        this.cachedCountDown=new AtomicInteger(bucketSize);

    }
    @Override
    public void onRemove(long bucketId, TLongLongHashMap map) {

    }

    @Override
    public void onMissCache(long id) {

    }

    @Override
    public void onCache(long bucketId, TLongLongHashMap map) {
        lru.put((int)bucketId,map);
        if(cachedCountDown.decrementAndGet()==0){
            latch.countDown();
            logger.info("index cache completely");
        }
    }
}
