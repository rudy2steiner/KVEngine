package com.alibabacloud.polar_race.engin;
import com.alibabacloud.polar_race.engine.common.AbstractEngine;
import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.utils.Bytes;
import com.alibabacloud.polar_race.example.ExampleEngine;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class EngineTest {
    private final static Logger logger= LoggerFactory.getLogger(EngineTest.class);
    int concurrency=64;
    private int numPerThreadWrite=100000;
    private byte[] values;
    private String template="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    Random random;
    private int VALUES_MAX_LENGTH=4096;
    AbstractEngine engine;
    @Before
    public void init(){
        random=new Random(0);
        int len=template.length();
        values=new byte[4096];
        for(int i=0;i<4096;i++){
            values[i]=(byte) template.charAt(random.nextInt(len));
        }
        try {
            engine = new ExampleEngine();
            engine.open("/export/rocksdb003/");
        }catch (EngineException e){
            logger.info("engine starter",e);
        }
    }
    @After
    public void close(){
        engine.close();
    }
    @Test
    public void benchmark8b4kbWrite(){
        logger.info(new String(values));
        long start=System.currentTimeMillis();
        Thread[] t=new Thread[concurrency];
         for (int i = 0; i < concurrency; i++) {
                t[i]=new Thread(new PutThread(i, numPerThreadWrite, engine));
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
    @Test
    public void benchmark8b4kbRead(){
        logger.info(new String(values));
        long start=System.currentTimeMillis();
        Thread[] t=new Thread[concurrency];
        for (int i = 0; i < concurrency; i++) {
            t[i]=new Thread(new GetThread(i, numPerThreadWrite, engine));
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

    @Test
    public void iterate(){
        byte[] lower=new byte[8];
        Bytes.long2bytes(0,lower,0);
        byte[] upper=new byte[8];
        Bytes.long2bytes(concurrency*numPerThreadWrite,upper,0);
        try {
            engine.range(lower, upper, new LongVisitor());
        }catch (EngineException e){
            logger.info("engine range",e);
        }

    }

    public class LongVisitor extends AbstractVisitor{
        @Override
        public void visit(byte[] key, byte[] value) {
            logger.info(String.format("%d,k:%s ,v:%d,%s",Thread.currentThread().getId(),Bytes.bytes2long(key,0),value.length,new String(value)));
        }
    }





    public class PutThread implements Runnable{
        private int id;
        private int num;
        private AbstractEngine engine;
        private byte[] values;
        private Random random;
        public PutThread(int id,int num,AbstractEngine engine){
            this.id=id;
            this.num=num;
            this.engine=engine;
            init();
        }
        public void init(){
            random=new Random(0);
            int len=template.length();
            values=new byte[4096];
            for(int i=0;i<4096;i++){
                values[i]=(byte) template.charAt(random.nextInt(len));
            }
        }
        /**
         * value 中隐藏key 的信息
         * 
         **/
        public void run()  {
            int  i=0;
            long key;
            byte[] keyBytes=new byte[8];
            int keyOffset;
            try {
                while (i < num) {
                    key = id * num + i;
                    Bytes.long2bytes(key, keyBytes, 0);
                    keyOffset = (int) (key % VALUES_MAX_LENGTH);
                    keyOffset = keyOffset < VALUES_MAX_LENGTH - 8 ? keyOffset : VALUES_MAX_LENGTH - 8;
                    for (int k = 0; k < 8; k++) {
                        values[keyOffset + k]=keyBytes[k];
                    }
                    engine.write(keyBytes, values);
                    if(i%10000==0){
                        logger.info(String.format("%d write key:%s",id,key));
                    }
                    i++;
                }
            }catch (EngineException e){
                logger.info(String.format("thread %d",id),e);
            }
        }
    }
    public class GetThread implements Runnable{
        private int id;
        private int num;
        private AbstractEngine engine;

        public GetThread(int id,int num,AbstractEngine engine){
            this.id=id;
            this.num=num;
            this.engine=engine;

        }

        /**
         * value 中隐藏key 的信息
         *
         **/
        public void run()  {
            int i=0;
            long key;
            byte[] keyBytes=new byte[8];
            byte[] values;
            int keyOffset;
            long value;
            try {
                while (i < num) {
                    key = id * num + i;
                    Bytes.long2bytes(key, keyBytes, 0);
                    values=engine.read(keyBytes);
                    keyOffset = (int) (key % VALUES_MAX_LENGTH);
                    keyOffset = keyOffset < VALUES_MAX_LENGTH - 8 ? keyOffset : VALUES_MAX_LENGTH - 8;
                    value=Bytes.bytes2long(values,keyOffset);
                    if(value!=key){
                        logger.error(String.format("%d,%d %d",id,key,value));
                    }else{
                        logger.info(String.format("%d,%d %d",id,key,value));
                    }
                    i++;
                }
            }catch (EngineException e){
                logger.info(String.format("thread %d",id),e);
            }
        }
    }



    public long keyGenerator(int id,int num,int position){

        return id*num+position;
    }

    @Test
    public void keyTest(){

        for(int i=0;i<concurrency;i++){
            for(int p=0;p<numPerThreadWrite;p++){
                logger.info(String.valueOf(keyGenerator(i,numPerThreadWrite,p)));
            }
            logger.info("break");
        }

    }

    @Test
    public void byteSum(){
        byte b=(byte)128;
        byte a=(byte)5;
        byte c=(byte) (a+b);
        byte d=(byte)(c-a);
        logger.info(String.valueOf((byte)(b)));

    }
}
