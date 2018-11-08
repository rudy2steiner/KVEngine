package com.alibabacloud.polar_race.engin;

import com.google.common.cache.*;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

@Ignore
public class LruTest {
    private final static Logger logger= LoggerFactory.getLogger(LruTest.class);

    @Test
    public void lru(){
        LoadingCache<Long,Long> lru=CacheBuilder.newBuilder()
                .maximumSize(10)
                .removalListener(new RemovalListener<Long, Long>() {
                    @Override
                    public void onRemoval(RemovalNotification<Long, Long> removalNotification) {
                        logger.info(String.format(" time %d,remove %d ,%d",System.nanoTime(),removalNotification.getKey(),removalNotification.getValue()));
                    }
                })
                .build(new CacheLoader<Long, Long>() {
                    @Override
                    public Long load(Long key) throws Exception {
                        logger.info(String.format("tim %d,load %d",System.nanoTime(),key));
                        return key;
                    }
                });
        try {
            for (long i = 0; i < 20; i++) {
                    lru.get(i);
            }

            System.out.println("get ");
        }catch (ExecutionException e){
            logger.info("execute exception",e);
        }



    }
}
