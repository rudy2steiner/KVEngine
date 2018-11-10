package com.alibabacloud.polar_race.engine.kv.cache;

import com.google.common.cache.LoadingCache;
import gnu.trove.map.hash.TLongLongHashMap;

public class IndexCacheListener implements CacheListener<TLongLongHashMap>{
    LoadingCache<Integer, TLongLongHashMap> lru;
    public IndexCacheListener(LoadingCache<Integer, TLongLongHashMap> lru){
        this.lru=lru;
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
    }
}
