package com.alibabacloud.polar_race.engin;
import com.alibabacloud.polar_race.engine.common.AbstractEngine;
import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.common.EngineRace;
import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.utils.Bytes;
import com.alibabacloud.polar_race.engine.common.utils.Files;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
/**                  w    r
 * rocksdb  64*1w   25s
 *          64*10w  848s   233s
 *
 **/
public class EngineTest {
    private final static Logger logger= LoggerFactory.getLogger(EngineTest.class);
    long concurrency=64;
    private long numPerThreadWrite=1000;
    private byte[] values;
    private String template="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    Random random;
    private static String root="/export/wal000/";
    private int VALUES_MAX_LENGTH=4096;
    private static boolean local=false;
    AbstractEngine engine;
    @Before
    public void beforeAction(){
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
    @After
    public void close(){
        logger.info("kv store  close");
        engine.close();
    }

    @AfterClass
    public static void afterClass(){
        logger.info("class empty");
        // 本地测试
        if(!local)
            Files.emptyDirIfExist(root);
    }

    @Test
    public void benchmark8b4kbWrite(){
        logger.info(new String(values));
        long start=System.currentTimeMillis();
        Thread[] t=new Thread[(int)concurrency];
         for (int i = 0; i < concurrency; i++) {
                t[i]=new Thread(new PutThread(i, (int)numPerThreadWrite, engine),"write"+i);
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
        Thread[] t=new Thread[(int)concurrency];
        for (int i = 0; i < concurrency; i++) {
            t[i]=new Thread(new GetThread(i, (int)numPerThreadWrite, engine),"reader"+i);
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

    /**
     *
     * to do concurrent iterate
     *
     **/
    @Ignore
    @Test
    public void iterate(){
        long start=System.currentTimeMillis()-1;
        byte[] lower=new byte[8];
        Bytes.long2bytes(0,lower,0);
        byte[] upper=new byte[8];
        Bytes.long2bytes(concurrency*numPerThreadWrite,upper,0);
        try {
            engine.range(lower, upper, new LongVisitor());
        }catch (EngineException e){
            logger.info("engine range",e);
        }
        long end=System.currentTimeMillis();
        logger.info(String.format("time elapsed %d ms,qps %d",end-start,numPerThreadWrite*concurrency*1000/(end-start)));
    }

    /**
     * visit kv listener
     *
     **/
    public class LongVisitor extends AbstractVisitor{
        int count=0;
        @Override
        public void visit(byte[] key, byte[] value) {
            count++;
            if(count%1==0)
                logger.info(String.format("count %d ,%d,k:%s ,v:%d,%s",count,Thread.currentThread().getId(),Bytes.bytes2long(key,0),value.length,new String(value)));
        }
    }


    public class PutThread implements Runnable{
        private int id;
        private int num;
        private AbstractEngine engine;
        private byte[] values;
        private byte[] keyBytes;
        private byte[] vals;
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
            keyBytes=new byte[StoreConfig.KEY_SIZE];
            vals=new byte[StoreConfig.VALUE_SIZE];
        }
        /**
         * value 中隐藏key 的信息
         * 
         **/
        public void run()  {
            int  i=0;
            long key;
            int keyOffset;
            try {
                while (i < num) {
                    key = id * num + i;
                    Bytes.long2bytes(key, keyBytes, 0);
                    keyOffset = (int) (key % VALUES_MAX_LENGTH);
                    keyOffset = keyOffset < VALUES_MAX_LENGTH - 8 ? keyOffset : VALUES_MAX_LENGTH - 8;
                    System.arraycopy(values,0,vals,0,vals.length);
                    for (int k = 0; k < 8; k++) {
                        vals[keyOffset + k]=keyBytes[k];
                    }
                    engine.write(keyBytes, vals);
                    i++;
                    if(i%10000==0){
                        logger.info(String.format("%d write key:%s",id,Bytes.bytes2long(keyBytes,0)));
                    }
                }
                logger.info(String.format("%d write finish",id));
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
            int success=0;
            try {
                while (i < num) {
                    key = id * num + i;
                    Bytes.long2bytes(key, keyBytes, 0);
                    values=engine.read(keyBytes);
                    keyOffset = (int) (key % VALUES_MAX_LENGTH);
                    keyOffset = keyOffset < VALUES_MAX_LENGTH - 8 ? keyOffset : VALUES_MAX_LENGTH - 8;
                    value=Bytes.bytes2long(values,keyOffset);
                    if(value!=key){
                        logger.error(String.format("%d,%d %d %s",id,key,value,new String(values)));
                    }else{
                        success++;
                        if(success%1000==0)
                                logger.info(String.format("%d,%d %d",id,key,value));
                    }
                    i++;
                }
            }catch (EngineException e){
                logger.info(String.format("thread %d",id),e);
            }
        }
    }

}
