package com.alibabacloud.polar_race.engine.kv.cache;

public interface CacheListener<T> {

    void onRemove(Integer bucketId,T map);
    void onCache(Integer bucketId,T map );
}
