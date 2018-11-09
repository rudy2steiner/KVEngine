package com.alibabacloud.polar_race.engine.kv.cache;

public interface CacheListener<T> {

    void onMissCache(long id);
    void onRemove(long id,T map);
    void onCache(long id,T map );
}
