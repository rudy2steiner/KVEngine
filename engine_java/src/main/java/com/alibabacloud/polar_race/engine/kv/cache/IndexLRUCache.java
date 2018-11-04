package com.alibabacloud.polar_race.engine.kv.cache;

import com.alibabacloud.polar_race.collection.LongLongMap;
import com.google.common.cache.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class IndexLRUCache {
    private final static Logger logger= LoggerFactory.getLogger(IndexLRUCache.class);
    LoadingCache<Integer, LongLongMap> lru;
    public IndexLRUCache(int cacheSize){
        this.lru= CacheBuilder.newBuilder()
                    .maximumSize(cacheSize)
                    .removalListener(new IndexRemoveListener())
                    .build(new IndexMapLoad());
    }


    /**
     * @param key
     * @return  value
     */
    public byte[] get(long key){
        return new byte[0];
        
    }



    public class IndexRemoveListener<Integer,LongLongMap> implements RemovalListener<Integer,LongLongMap>{

        @Override
        public void onRemoval(RemovalNotification<Integer, LongLongMap> removalNotification) {
             logger.info(String.format("remove %d",removalNotification.getKey()));
        }
    }

    public class IndexMapLoad extends CacheLoader<Integer,LongLongMap>{

        public IndexMapLoad(){


        }
        @Override
        public LongLongMap load(Integer integer) throws Exception {
            return null;
        }
    }




}
