package com.alibabacloud.polar_race.engin;

import com.alibabacloud.polar_race.engine.common.collection.LongLongMap;
import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.common.io.IOHandler;
import com.alibabacloud.polar_race.engine.common.utils.Memory;
import com.alibabacloud.polar_race.engine.kv.event.TaskBus;
import com.alibabacloud.polar_race.engine.kv.file.LogFileService;
import com.alibabacloud.polar_race.engine.kv.file.LogFileServiceImpl;
import com.google.common.cache.*;
import gnu.trove.map.hash.TLongLongHashMap;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;


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
        TaskBus closeHandlerProcessor=new TaskBus(1);
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

    /**
     *   miss 次数超过阈值，
     **/
    @Test
    public void missFrequencyLoad(){
        BitSet inMemory=new BitSet(100);
        short[] miss=new short[100];
        int limit=4;
        int max=1000;
        Random random=new Random(0);
        while (max-->0){
            int visit=random.nextInt(100);
               if(!inMemory.get(visit)) {
                   miss[visit] += 1;
                   if (miss[visit] > limit) {
                       logger.info(String.format("%d miss reach upper limit,load ", visit));
                       inMemory.set(visit);
                   }
               }
        }

        PriorityQueue priorityQueue;

    }

    @Test
    public void primitiveInt(){

    }

    @Test
    public void primitiveMapMemory(){
        long start=0;
        long end=10*1024*1024;
        /*1kw*/
        LongLongMap rateMap = LongLongMap.withExpectedSize(1000_0000);
        for(long i=start;i<end;i++){
            rateMap.put(i,i);
        }
        try {
            Thread.sleep(100000);
        }catch (InterruptedException e){

        }
        logger.info("stop");
    }

    @Test
    public void troveMap(){
        Memory.limit(3*512*1024*1024);
        long start=0;
        long end=1000*1000;
        int max=128;
        TLongLongHashMap[] maps=new TLongLongHashMap[max];
        for(int index=0;index<max;index++) {
            maps[index]=new TLongLongHashMap((int) ((1000000 + 100) / 0.98), 0.98f);
            for (long i = start; i < end; i++) {
                maps[index].put(i, i);
            }
        }
        try {
            logger.info("stop "+(int)((64000000+100)/0.9)*16);
            Thread.sleep(100000);
        }catch (InterruptedException e){

        }

    }


}
