package com.alibabacloud.polar_race.engin;

import com.alibabacloud.polar_race.engine.common.collection.LongLongMap;
import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.common.io.IOHandler;
import com.alibabacloud.polar_race.engine.common.utils.Bytes;
import com.alibabacloud.polar_race.engine.common.utils.Memory;
import com.alibabacloud.polar_race.engine.kv.event.TaskBus;
import com.alibabacloud.polar_race.engine.kv.file.LogFileService;
import com.alibabacloud.polar_race.engine.kv.file.LogFileServiceImpl;
import com.alibabacloud.polar_race.engine.kv.index.Index;
import com.alibabacloud.polar_race.engine.kv.partition.LexigraphicalPartition;
import com.alibabacloud.polar_race.engine.kv.partition.Range;
import com.carrotsearch.hppc.LongLongHashMap;
import com.google.common.cache.*;
import gnu.trove.map.hash.TLongLongHashMap;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.rocksdb.HashSkipListMemTableConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zzz_koloboke_compile.shaded.org.$slf4j$.helpers.FormattingTuple;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.LongStream;


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
        int max=64;
        TLongLongHashMap[] maps=new TLongLongHashMap[max];
        for(int index=0;index<max;index++) {
            maps[index]=new TLongLongHashMap((int) ((1000000 + 100) / 0.98), 0.98f);
            for (long i = start; i < end; i++) {
                maps[index].put(i, i);
            }

        }
        try {
            Thread.sleep(100000);
        }catch (InterruptedException e){

        }


    }
    @Test
    public void hppcMap(){
        Memory.limit(3*512*1024*1024);
        long start=0;
        long end=1000*1000;
        int max=64;
        LongLongHashMap[] maps=new LongLongHashMap[max];
        for(int index=0;index<max;index++) {
            maps[index]=new LongLongHashMap((int) ((1000000 + 100) / 0.98), 0.98f);
            for (long i = start; i < end; i++) {
                maps[index].put(i, i);
            }

        }
        try {
            Thread.sleep(100000);
        }catch (InterruptedException e){

        }
//        maps[index].put(-7,-1);
//        long value= maps[index].get(-7);

        logger.info("key 0 value");
    }

    @Test
    public void lexigraphicalCompare(){
        Random random=new Random();
        long start=System.currentTimeMillis();
        int  size=60000000;
        int initPartitionCapacity=1010000;
        LexigraphicalPartition partition=new LexigraphicalPartition(Long.MIN_VALUE,Long.MAX_VALUE,64,initPartitionCapacity);
        //long[] arr=new long[size];
        int slotId;
        long value;
        Range range=null;
        Index[] unsortIndex=new Index[initPartitionCapacity];
        while(--size>0){
            value=random.nextLong();
            slotId=partition.partition(value);
            range=partition.getPartition(slotId);
            range.add(new Index(value,(int)value));
            if(size%1000000==0)
                logger.info(String.format("%d partition %s,%d",value,range.toString(), range.contain(value)));
        }
        logger.info(String.format("stop %d ms",System.currentTimeMillis()-start));

        // sort 135
        //
        Index[] keys=range.getSlot();
//        int i=0;
//        for(Index key:keys){
//             unsortIndex[i++]=new Index(key,(int)key);
//        }
        start=System.currentTimeMillis();
        Arrays.sort(unsortIndex, 0, range.getSize(), new Comparator<Index>() {
            @Override
            public int compare(Index o1, Index o2) {
               long diff=o1.getKey()-o2.getKey();
               if(diff==0) return 0;
               if(diff>0) return 1;
               else return -1;
            }
        });

        logger.info(String.format("generic sort stop %d ms",System.currentTimeMillis()-start));
        start=System.currentTimeMillis();
        Arrays.sort(keys,0,range.getSize());
        logger.info(String.format("sort stop %d ms",System.currentTimeMillis()-start));
        sample(range.getSlot(),0,range.getSize(),10000);
        NavigableMap<Long,Integer> navigableMap;
        SortedMap<Long,Integer> sortedMap;
        ConcurrentSkipListMap skipListMap;
       // HashSkipListMemTableConfig;
        //SkipList skipList;
        //Arrays.binarySearch()
    }

    public void sample(Index[] array,int start,int end,int mode){
        for(int i=start;i<end;i++){
            if(i%mode==0){
                logger.info(""+array[i].getKey());
            }
        }
    }




}
