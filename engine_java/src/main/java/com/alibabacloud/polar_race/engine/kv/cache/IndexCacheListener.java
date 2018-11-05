package com.alibabacloud.polar_race.engine.kv.cache;

import com.alibabacloud.polar_race.collection.LongLongMap;
import com.google.common.cache.LoadingCache;

public class IndexCacheListener implements CacheListener<LongLongMap>{
    LoadingCache<Integer, LongLongMap> lru;
    public IndexCacheListener(LoadingCache<Integer, LongLongMap> lru){
        this.lru=lru;
    }
    @Override
    public void onRemove(Integer bucketId, LongLongMap map) {

    }

    @Override
    public void onCache(Integer bucketId, LongLongMap map) {
        lru.put(bucketId,map);
    }
}
