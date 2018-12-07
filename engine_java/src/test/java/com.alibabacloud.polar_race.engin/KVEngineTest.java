package com.alibabacloud.polar_race.engin;

import com.alibabacloud.polar_race.engine.common.AbstractEngine;
import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.common.EngineRace;
import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.utils.Bytes;
import com.alibabacloud.polar_race.engine.common.utils.Memory;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

@Ignore
public class KVEngineTest {
    private final static Logger logger= LoggerFactory.getLogger(KVEngineTest.class);
    int concurrency=64;
    private int numPerThreadWrite=500000;
    private byte[] values;
    private long[] keys;
    private String template="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    Random random;
    private final static int VALUES_MAX_LENGTH=4096;
    private static String root="/export/wal000";
    AbstractEngine engine;
    @Before
    public void beforeAction(){
        long memorySize=(long)6*512*1024*1024;
        Memory.limit(memorySize);
        random=new Random(0);
        int len=template.length();
        values=new byte[4096];
        for(int i=0;i<4096;i++){
            values[i]=(byte) template.charAt(random.nextInt(len));
        }
        try {
            engine = new EngineRace();
            engine.open(root);
            logger.info("kv store started");
        }catch (EngineException e){
            logger.info("engine start error",e);
        }

    }


    public void initKeyArray(){
        long start=System.currentTimeMillis();
        long lower=Long.MIN_VALUE;
        long upper=Long.MAX_VALUE;
        int totalKey=concurrency*numPerThreadWrite;
        long step=upper/(totalKey/2);
        keys=new long[totalKey+1];
        long key=0;
        int i=0;
        for(;i<totalKey/2;i++){
            keys[i]=key;
            key+=step;
        }
        // start from mini long
        key=lower;
        for(;i<totalKey;i++){
            keys[i]=key;
            key+=step;
        }
        keys[totalKey]=-1;
        logger.info("key set init,take time:"+(System.currentTimeMillis()-start));
    }


    @After
    public void close(){
        logger.info("kv store  asyncClose");
        engine.close();
    }

    @Test
    public void startRangeKVPut(){
        logger.info(new String(values));
        initKeyArray();
        long start=System.currentTimeMillis();
        Thread[] t=new Thread[concurrency];
        for (int i = 0; i < concurrency; i++) {
            t[i]=new Thread(new RangePutThread(i*numPerThreadWrite,Math.min(concurrency*numPerThreadWrite,(i+1)*numPerThreadWrite),numPerThreadWrite,i),"write"+i);
            t[i].start();
        }
        try {
            for (int i = 0; i < concurrency; i++) {
                t[i].join();
            }
        }catch (InterruptedException e){
            e.printStackTrace();
        }
        long end=System.currentTimeMillis();
        logger.info(String.format("time elapsed %d ms,qps %d",end-start,(long)numPerThreadWrite*concurrency*1000/(end-start)));
    }

    @Test
    public void startGetKV(){
        //logger.info(new String(values));
        initKeyArray();
        long start=System.currentTimeMillis();
        Thread[] t=new Thread[concurrency];
        for (int i = 0; i < concurrency; i++) {
            t[i]=new Thread(new RangeGetKVThread(i*numPerThreadWrite,Math.min(concurrency*numPerThreadWrite,(i+1)*numPerThreadWrite),numPerThreadWrite,i),"write"+i);
            t[i].start();
        }
        try {
            for (int i = 0; i < concurrency; i++) {
                t[i].join();
            }
        }catch (InterruptedException e){
            e.printStackTrace();
        }
        long end=System.currentTimeMillis();
        logger.info(String.format("time elapsed %d ms,qps %d",end-start,(long)numPerThreadWrite*concurrency*1000/(end-start)));
    }

    @Test
    public void startRangeIterate(){
        //logger.info(new String(values));
        long start=System.currentTimeMillis();
        Thread[] t=new Thread[concurrency];
        for (int i = 0; i < concurrency; i++) {
            t[i]=new Thread(new RangeIterateThread(i*numPerThreadWrite,Math.min(concurrency*numPerThreadWrite,(i+1)*numPerThreadWrite),numPerThreadWrite,i),"write"+i);
            t[i].start();
        }
        try {
            for (int i = 0; i < concurrency; i++) {
                t[i].join();
            }
        }catch (InterruptedException e){
            e.printStackTrace();
        }
        long end=System.currentTimeMillis();
        logger.info(String.format("time elapsed %d ms,qps %d",end-start,numPerThreadWrite*concurrency*1000/(end-start)));
    }
    public class RangePutThread implements Runnable{
        private int start;
        private int end;
        private int totalKey;
        private byte[] keyBytes;
        private byte[] vals;
        private int tid;
        Random  rand=new Random(0);
        public RangePutThread(int start,int end,int totalkey,int id){
            this.start=start;
            this.end=end;
            this.totalKey=totalkey;
            this.tid=id;
            init();
        }
        public void init(){
            keyBytes=new byte[StoreConfig.KEY_SIZE];
            vals=new byte[StoreConfig.VALUE_SIZE];
        }
        @Override
        public void run() {
              long key;
                int keyOffset;
                try{
                    for(int i=start;i<end;i++){
                    key=keys[i];
                    Bytes.long2bytes(key, keyBytes, 0);
                    keyOffset = Math.abs((int) (key % VALUES_MAX_LENGTH));
                    keyOffset = keyOffset < VALUES_MAX_LENGTH - 8 ? keyOffset : VALUES_MAX_LENGTH - 8;
                    System.arraycopy(values,0,vals,0,vals.length);
                    for (int k = 0; k < 8; k++) {
                      vals[keyOffset + k]=keyBytes[k];
                    }
                    engine.write(keyBytes, vals);
                    }
                    logger.info(String.format("thread %d,[%d,%d] write finish",tid,start,end));
            }catch (EngineException e){
                    logger.info(String.format("thread %d,[%d,%d] write error",tid,start,end),e);
            }
        }
    }

    public class RangeGetKVThread implements Runnable{
        private int start;
        private int end;
        private int totalKey;
        private byte[] keyBytes;
        private byte[] vals;
        private int tid;
        public RangeGetKVThread(int start,int end,int totalkey,int id){
            this.start=start;
            this.end=end;
            this.totalKey=totalkey;
            this.tid=id;
            init();
        }
        public void init(){
            keyBytes=new byte[StoreConfig.KEY_SIZE];
        }
        @Override
        public void run() {
            long key;
            int keyOffset;
            int notFound=0;
            int success=0;
            int failed=0;
            boolean hasValue=false;
            long value;

            for(int i=start;i<end;i++){
                    key=keys[i];
                    Bytes.long2bytes(key, keyBytes, 0);
                    try {
                        vals = engine.read(keyBytes);
                    }catch (Exception e){
                        notFound++;
                        logger.info("read exception,ignore ", e);
                        //i++;
                        continue;
                    }
                    keyOffset = Math.abs((int) (key % VALUES_MAX_LENGTH));
                    keyOffset = keyOffset < VALUES_MAX_LENGTH - 8 ? keyOffset : VALUES_MAX_LENGTH - 8;
                    value=Bytes.bytes2long(vals,keyOffset);
                    if(value!=key){
                        failed++;
                        //if(failed%10==0)
                           logger.error(String.format("%d,%d %d %s",tid,key,value,hasValue?new String(values):""));
                    }else{
                        success++;
                        if(success%10000==0)
                            logger.info(String.format("%d,%d %d",tid,key,value));
                    }
            }
            logger.info(String.format("[%d,%d] read kv finish,success %d",start,end,success));
        }
    }




    public class RangeIterateThread implements Runnable{
        private int start;
        private int end;
        private int totalKey;
        private byte[] keyBytes;
        private int tid;

        public RangeIterateThread(int start,int end,int totalkey,int id){
            this.start=start;
            this.end=end;
            this.totalKey=totalkey;
            this.tid=id;
            init();
        }
        public void init(){
            keyBytes=new byte[StoreConfig.KEY_SIZE];
        }
        @Override
        public void run() {
            byte[] lowerByte=new byte[8];
            byte[] upperByte=new byte[8];
            Bytes.long2bytes(keys[start],lowerByte,0);
            Bytes.long2bytes(keys[end],upperByte,0);
            try {
                engine.range(lowerByte, upperByte, new AbstractVisitor() {
                    int index=start;
                    long key;
                    long value;
                    int keyOffset;
                    int success=0;
                    int failed=0;
                    @Override
                    public void visit(byte[] keyByte, byte[] values) {
                        key=Bytes.bytes2long(keyByte,0);
                        keyOffset = Math.abs((int) (key % VALUES_MAX_LENGTH));
                        keyOffset = keyOffset < VALUES_MAX_LENGTH - 8 ? keyOffset : VALUES_MAX_LENGTH - 8;
                        value=Bytes.bytes2long(values,keyOffset);
                        if(value!=key){
                            failed++;
                            // offset error
                            logger.error(String.format("tid %d,offset error,%d %d %s",tid,key,value,new String(values)));
                        }else{
                            if(keys[index]!=key){
                                throw new IllegalArgumentException("unexpected kv");
                            }
                            success++;
                            if(success%10000==0)
                                logger.info(String.format("tid %d,%d %d,successed %d",tid,key,value,success));

                        }
                        index++;
                    }
                });
            }catch (EngineException e){
                e.printStackTrace();
                logger.info(String.format("[%d,%d] iterate error",start,end));
            }
            logger.info(String.format("[%d,%d] iterate finish",start,end));
        }
    }











}
