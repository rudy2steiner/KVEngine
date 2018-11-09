package com.alibabacloud.polar_race.engin;

import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.common.io.IOHandler;
import com.alibabacloud.polar_race.engine.kv.event.Cell;
import com.alibabacloud.polar_race.engine.kv.event.EventBus;
import com.alibabacloud.polar_race.engine.kv.file.LogFileService;
import com.alibabacloud.polar_race.engine.kv.file.LogFileServiceImpl;
import com.google.common.cache.*;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.ByteBuffer;
import java.util.List;


@Ignore
public class LruTest {
    private final static Logger logger= LoggerFactory.getLogger(LruTest.class);
    private String root="/export/wal000/wal/";
    @Test
    public void lru(){
        LoadingCache<Long,Long> lru=CacheBuilder.newBuilder()
                .maximumSize(10)
                .removalListener(new RemovalListener<Long, Long>() {
                    @Override
                    public void onRemoval(RemovalNotification<Long, Long> removalNotification) {
                        logger.info(String.format(" time %d,remove %d ,%d",System.nanoTime(),removalNotification.getKey(),removalNotification.getValue()));
                    }
                }).recordStats()
                .build(new CacheLoader<Long, Long>() {
                    @Override
                    public Long load(Long key) throws Exception {
                        logger.info(String.format("tim %d,load %d",System.nanoTime(),key));
                        return key;
                    }
                });
           // put
            for (long i = 0; i < 20; i++) {
                    lru.put(i,i);
            }
            for (long i = 0; i < 20; i++) {
             Long  value= lru.getIfPresent(i);
                if(value!=null)
                 System.out.println("get "+value);
            }

    }

    LogFileService fileService;
    @Before
    public void beforeAction(){
        EventBus closeHandlerProcessor=new EventBus(1);
        fileService=new LogFileServiceImpl(root,closeHandlerProcessor);

    }

    /**
     * 71057 ms time ,640w kv 文件
     *    7000ms          4K
     *
     **/
    @Test
    public void randomReadBucket(){
        int readSize=fileService.logWritableSize();
        ByteBuffer buffer=ByteBuffer.allocateDirect(readSize);
        List<Long> files=fileService.allLogFiles();
        IOHandler handler;
        long startTime=System.currentTimeMillis();
        try {
            int i=0;
            for (long fid : files) {
                buffer.clear();
                handler = fileService.ioHandler(fid + StoreConfig.LOG_FILE_SUFFIX);
                if(handler.length()>=readSize){
                    handler.read(buffer);
                    buffer.flip();
                    if(i++%1000==0)
                        logger.info(String.format("%d file,read %d",fid,buffer.remaining()));
                }

            }
        }catch (Exception e){
            logger.info("not found",e);
        }finally {
            logger.info(String.format("%d ms time",System.currentTimeMillis()-startTime));
        }
    }
}
