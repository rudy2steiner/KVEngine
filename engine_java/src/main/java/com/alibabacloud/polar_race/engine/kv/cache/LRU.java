package com.alibabacloud.polar_race.engine.kv.cache;

import java.util.LinkedHashMap;
import java.util.Map;

public class LRU<K,V> extends LinkedHashMap<K,V> {

    private final int MAX_CACHE_SZIE;
    private CacheListener<K,V> cacheListener;
    public LRU(int cacheSize,CacheListener<K,V> listener){
        super((int)Math.ceil(cacheSize/0.75)+1,0.75f,true);
        this.cacheListener=listener;
        this.MAX_CACHE_SZIE=cacheSize;
    }


    @Override
    public V put(K key, V value) {
        cacheListener.onCache(key,value);
        return super.put(key, value);
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        boolean remove=size()>MAX_CACHE_SZIE;
        if(remove){
            cacheListener.onRemove(eldest.getKey(),eldest.getValue());
        }
        return remove;
    }


}
