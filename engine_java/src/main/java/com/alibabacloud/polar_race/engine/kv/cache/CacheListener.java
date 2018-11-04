package com.alibabacloud.polar_race.engine.kv.cache;

import java.util.Map;

public interface CacheListener<K,V> {

    void onRemove(K k,V v);
    void onCache(K k,V v );
}
