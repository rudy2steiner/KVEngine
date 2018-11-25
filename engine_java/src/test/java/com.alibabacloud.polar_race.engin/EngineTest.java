package com.alibabacloud.polar_race.engin;
import com.alibabacloud.polar_race.engine.common.AbstractEngine;
import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.common.EngineRace;
import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.utils.Bytes;
import com.alibabacloud.polar_race.engine.common.utils.Files;
import com.alibabacloud.polar_race.engine.common.utils.Memory;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**                  w    r
 * rocksdb  64*1w   25s
 *          64*10w  848s   233s
 *
 **/
@Ignore
public class EngineTest {
    private final static Logger logger= LoggerFactory.getLogger(EngineTest.class);
    int concurrency=64;
    private long numPerThreadWrite=10000;

    private long keyValueOffset=-16000000;  // default
    private byte[] values;
    private long[] keys;
    private String template="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    Random random;
    private static String root="/export/wal000";
    private int VALUES_MAX_LENGTH=4096;
    private static boolean local=true;
    private byte one=0b00000001;
    private byte two=0b00000010;
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
        initKeyArray();
    }

    @Test
    public void initKeyArray(){
        long lower=Long.MIN_VALUE;
        long upper=Long.MAX_VALUE;
        int totalKey=concurrency*(int)numPerThreadWrite;
        long step=upper/(totalKey/2);
        keys=new long[totalKey];
        long key=lower;
        for(int i=0;i<totalKey;i++){
           keys[i]=key;
           key+=step;
        }
        logger.info("key set init");
    }
    @After
    public void close(){
        logger.info("kv store  asyncClose");
        engine.close();
    }

    @AfterClass
    public static void afterClass(){
        // 本地测试
        if(!local) {
            logger.info("empty "+root);
            Files.emptyDirIfExist(root);
        }else{
            logger.info("local model");
        }
    }

    @Test
    public void benchmark8b4kbWrite(){
        logger.info(new String(values));
        long start=System.currentTimeMillis();
        Thread[] t=new Thread[(int)concurrency];
         for (int i = 0; i < concurrency; i++) {
                t[i]=new Thread(new UniquePutThread(i, (int)numPerThreadWrite, engine),"write"+i);
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
     * 整个long 区间
     **/
    @Test
    public void benchmark8b4kbRangeRead(){
        logger.info(new String(values));
        long start=System.currentTimeMillis();
        long startkey=Long.MIN_VALUE;
        long endKey=Long.MAX_VALUE;
        long span=endKey/(concurrency/2);
        Thread[] t=new Thread[(int)concurrency];
        for (int i = 0; i < concurrency; i++) {
            t[i]=new Thread(new GetRangKeyThread(i,startkey+ i*span,startkey+(i+1)*span, engine),"write"+i);
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
    public void benchmark8b4kbRangeWrite(){
        logger.info(new String(values));
        long start=System.currentTimeMillis();
        long startkey=Long.MIN_VALUE;
        long endKey=Long.MAX_VALUE;
        long span=endKey/(concurrency/2);
        Thread[] t=new Thread[(int)concurrency];
        long startx[]=new long[concurrency];
        long end[]=new long[concurrency];
        Runnable task;
        for (int i = 0; i < concurrency; i++) {
            startx[i]=startkey+ i*span;
            end[i]=startkey+(i+1)*span;
        }
        for (int i = 0; i < concurrency; i++) {
            task=new UniqueRangeKeyPutThread(i,startx[i],end[i], engine);
            t[i]=new Thread(task,"write"+i);
            t[i].start();
        }
        try {
            for (int i = 0; i < concurrency; i++) {
                t[i].join();
            }
        }catch (InterruptedException e){
            e.printStackTrace();
        }
        long endt=System.currentTimeMillis();
        logger.info(String.format("time elapsed %d ms,qps %d",endt-start,numPerThreadWrite*concurrency*1000/(endt-start)));
    }

    /**
     *  线程内flip
     **/
    @Test
    public void benchmark8b4kbFlipWrite(){
        logger.info(new String(values));
        long start=System.currentTimeMillis();
        Thread[] t=new Thread[(int)concurrency];
        for (int i = 0; i < concurrency; i++) {
            t[i]=new Thread(new FlipKeyThread(i, (int)numPerThreadWrite, engine),"write"+i);
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
     *  跨线程key flip
     **/
    @Test
    public void benchmark8b4kbFlipOutWrite(){
        logger.info(new String(values));
        long start=System.currentTimeMillis();
        Thread[] t=new Thread[(int)concurrency];
        for (int i = 0; i < concurrency; i++) {
            t[i]=new Thread(new FlipKeyOutPutThread(i, (int)numPerThreadWrite, engine),"write"+i);
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
     *  复合flip
     **/
    @Test
    public void complex8b4kFlipOutWrite(){

        logger.info(new String(values));
        long start=System.currentTimeMillis();
        Thread[] t=new Thread[(int)concurrency];
        for (int i = 0; i < concurrency; i++) {
            if(i%2==0) {
                t[i] = new Thread(new FlipKeyOutPutThread(i, (int) numPerThreadWrite, engine), "write" + i);
            }else{
                t[i] = new Thread(new FlipKeyThread(i, (int) numPerThreadWrite, engine), "write" + i);
            }
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


    /***
     * 唯一kv 写入线程
     **/
    public class UniquePutThread implements Runnable{
        private int id;
        private int num;
        private AbstractEngine engine;
        private byte[] values;
        private byte[] keyBytes;
        private byte[] vals;
        private Random random;
        public UniquePutThread(int id, int num,AbstractEngine engine){
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
                    key = keyValueOffset+id * num + i;
                    Bytes.long2bytes(key, keyBytes, 0);
                    keyOffset = Math.abs((int) (key % VALUES_MAX_LENGTH));
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

    /***
     * 唯一kv 写入线程
     **/
    public class FlipKeyThread implements Runnable{
        private int id;
        private int num;
        private AbstractEngine engine;
        private byte[] values;
        private byte[] keyBytes;
        private byte[] vals;
        private Random random;
        public FlipKeyThread(int id, int num, AbstractEngine engine){
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
                    key = keyValueOffset+id * num + i;
                    Bytes.long2bytes(key, keyBytes, 0);
                    keyOffset = Math.abs((int) (key % VALUES_MAX_LENGTH));
                    keyOffset = keyOffset < VALUES_MAX_LENGTH - 8 ? keyOffset : VALUES_MAX_LENGTH - 8;
                    System.arraycopy(values,0,vals,0,vals.length);
                    for (int k = 0; k < 8; k++) {
                         // flip 第八个bit
                        vals[keyOffset + k] = keyBytes[k];
                    }
                    if(random.nextDouble()<0.2){
//                        keyBytes[6]=(byte)(keyBytes[6]^one);
                        if(key%10000==0)
//                            logger.info(String.format("original %d,offset %d",key,Bytes.bytes2long(keyBytes,0)));
                        logger.info("key skip:"+key);
                        i++;
                        continue;
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

    /***
     *  跳出本线程的key范围
     **/
    public class FlipKeyOutPutThread implements Runnable{
        private int id;
        private int num;
        private AbstractEngine engine;
        private byte[] values;
        private byte[] keyBytes;
        private byte[] vals;
        private Random random;
        public FlipKeyOutPutThread(int id, int num,AbstractEngine engine){
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
                    if(random.nextDouble()<0.1){
                        key= id^two * num + i;
                    }else{
                        key = id * num + i;
                    }
                    Bytes.long2bytes(key, keyBytes, 0);
                    keyOffset = Math.abs((int) (key % VALUES_MAX_LENGTH));
                    keyOffset = keyOffset < VALUES_MAX_LENGTH - 8 ? keyOffset : VALUES_MAX_LENGTH - 8;
                    System.arraycopy(values,0,vals,0,vals.length);
                    for (int k = 0; k < 8; k++) {
                        // flip 第八个bit
                        vals[keyOffset + k] = keyBytes[k];
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
         * 处理runtime exception
         **/
        public void run()  {
            int i=0;
            long key;
            byte[] keyBytes=new byte[8];
            byte[] values;
            int keyOffset;
            long value;
            int success=0;
            int failed=0;
            int notFound=0;
            while (i < num) {
                    key = keyValueOffset+id * num + i;
                    Bytes.long2bytes(key, keyBytes, 0);
                    try {
                        values = engine.read(keyBytes);
                    }catch (Exception e){
                        notFound++;
                      if(notFound%10000==0) {
                          logger.info("read exception,ignore ", e);
                      }
                        i++;
                        continue;
                    }
                    keyOffset = Math.abs((int) (key % VALUES_MAX_LENGTH));
                    keyOffset = keyOffset < VALUES_MAX_LENGTH - 8 ? keyOffset : VALUES_MAX_LENGTH - 8;
                    value=Bytes.bytes2long(values,keyOffset);
                    if(value!=key){
                        failed++;
                        if(failed%10000==0)
                        logger.error(String.format("%d,%d %d %s",id,key,value,new String(values)));
                    }else{
                        success++;
                        if(success%10000==0)
                                logger.info(String.format("%d,%d %d",id,key,value));
                    }
                    i++;
            }
            logger.info(String.format("thread %d exit",id));

        }
    }

    /***
     * 校验 单增，没有遗漏key,且是在这个范围
     *
     **/
    public class RangeThread implements Runnable{

        private long lower;
        private long upper;
        private int expectedKeyCount;
        public RangeThread(long lower,long upper,int expectedKeyCount){
            this.lower=lower;
            this.upper=upper;
            this.expectedKeyCount=expectedKeyCount;
        }
        @Override
        public void run() {
            byte[] lowerByte=new byte[8];
            byte[] upperByte=new byte[8];
            Bytes.long2bytes(lower,lowerByte,0);
            Bytes.long2bytes(upper,upperByte,0);
            CountDownLatch visitLatch=new CountDownLatch(1);
            try{
             engine.range(lowerByte, upperByte, new AbstractVisitor() {
                 @Override
                 public void visit(byte[] key, byte[] value) {

                 }
             });
            }catch (EngineException e){
                e.printStackTrace();
            }
        }
    }
    public class GetRangKeyThread implements Runnable{
        private int id;
        private int num;
        private AbstractEngine engine;
        private long start;
        private long end;
        private long step;
        public GetRangKeyThread(int id,long start,long end,AbstractEngine engine){
            this.id=id;
            this.num=num;
            this.start=start;
            this.end=end;
            this.engine=engine;
            this.step=(end-start)/numPerThreadWrite;

        }

        /**
         * value 中隐藏key 的信息
         * 处理runtime exception
         **/
        public void run()  {
            long i=0;
            long key;
            byte[] keyBytes=new byte[8];
            byte[] values;
            int keyOffset;
            long value;
            int success=0;
            int failed=0;
            int notFound=0;
            key = start;
            while (i < numPerThreadWrite) {
                Bytes.long2bytes(key, keyBytes, 0);
                try {
                    values = engine.read(keyBytes);
                }catch (Exception e){
                    notFound++;
                    if(notFound%10000==0) {
                        logger.info("read exception,ignore ", e);
                    }
                    i++;
                    key+=step;
                    continue;
                }
                keyOffset = Math.abs((int) (key % VALUES_MAX_LENGTH));
                keyOffset = keyOffset < VALUES_MAX_LENGTH - 8 ? keyOffset : VALUES_MAX_LENGTH - 8;
                value=Bytes.bytes2long(values,keyOffset);
                if(value!=key){
                    failed++;
                    if(failed%10000==0)
                        logger.error(String.format("%d,%d %d %s",id,key,value,new String(values)));
                }else{
                    success++;
                    if(success%10000==0)
                        logger.info(String.format("%d,%d %d",id,key,value));
                }
                i++;
                key+=step;
            }
            logger.info(String.format("thread %d exit",id));
        }
    }

    /***
     * 唯一kv 写入线程
     **/
    public class UniqueRangeKeyPutThread implements Runnable{
        private int id;
        private int num;
        private AbstractEngine engine;
        private byte[] values;
        private byte[] keyBytes;
        private byte[] vals;
        private Random random;
        private long start;
        private long end;
        private long step;
        private long meet=1000000000000L;
        public UniqueRangeKeyPutThread(int id, long start,long end,AbstractEngine engine){
            this.id=id;
            this.start=start;
            this.end=end;
            this.step=(end-start)/numPerThreadWrite;
            this.engine=engine;
            init();
            logger.info(String.format("start %d,end %d, time %d",start,end,(end-start)/step));
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
            long  i=0;
            long key=start;
            int keyOffset;
            try {
                while ( i< numPerThreadWrite) {
                    Bytes.long2bytes(key, keyBytes, 0);
                    keyOffset = Math.abs((int) (key % VALUES_MAX_LENGTH));
                    keyOffset = keyOffset < VALUES_MAX_LENGTH - 8 ? keyOffset : VALUES_MAX_LENGTH - 8;
                    System.arraycopy(values,0,vals,0,vals.length);
                    for (int k = 0; k < 8; k++) {
                        vals[keyOffset + k]=keyBytes[k];
                    }
                    key+=step;
                    i++;
                    engine.write(keyBytes, vals);
                    if(i%meet==0){
                        logger.info(String.format("%d write key:%s",id,Bytes.bytes2long(keyBytes,0)));
                    }
                }
                logger.info(String.format("%d write finish",id));
            }catch (EngineException e){
                logger.info(String.format("thread %d",id),e);
            }
        }
    }

}


