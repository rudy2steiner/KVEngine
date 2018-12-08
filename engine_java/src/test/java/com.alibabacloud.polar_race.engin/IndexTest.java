package com.alibabacloud.polar_race.engin;

import com.alibabacloud.polar_race.engine.common.utils.Files;
import com.alibabacloud.polar_race.engine.kv.index.IndexHashAppender;
import com.alibabacloud.polar_race.engine.kv.wal.WALogger;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.BitSet;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * only test index hash  and index Engine
 *
 **/
@Ignore
public class IndexTest {
    private final static Logger logger= LoggerFactory.getLogger(IndexTest.class);
    private String root="/export/wal000/";
    @Test
    public void KeyIndexIterate() throws Exception{
        WALogger logger=new WALogger(root,null,null);
        logger.startAsyncHashTask();

    }

    @Test
    public void  testMax(){
        System.out.println(Math.max(2,3));
    }
    @Test
    public void IndexIterate() throws Exception{

        WALogger logger=new WALogger(root,null,null);
        logger.startAsyncIndexCacheTask();

    }

    @Test
    public void delDir(){
        logger.info("dir empty");
        Files.emptyDirIfExist(root);
    }

    @Test
    public void byteFilp(){
        BitSet bitSet;
        byte b=(byte)3;
        byte zero=(byte)1;
        byte c=(byte)(b^zero);
        System.out.println(c);
         c=(byte)(4^zero);
        System.out.println(c);

    }

    /**
     * hash 测试
     **/
    @Test
    public void hashTest(){
          long start=Long.MIN_VALUE;
          long end =Long.MAX_VALUE;
          int bucket=64;
          int n=bucket-1;
          for(long i=start;i<end;i++)
            logger.info(String.format("%d hash to bucket %d",i,IndexHashAppender.hash(i)&n));

    }

    @Test
    public  void shortIncrement(){
        short v=0;
        for(int i=0;i<10000;i++){
            v+=1;
            logger.info(v+"");
        }


    }

    @Test
    public void atomicInteger(){
         int v=Integer.MAX_VALUE-3;
        AtomicInteger a=new AtomicInteger(v);
        for(int i=0;i<1000;i++)
            logger.info(a.addAndGet(5)+"");
    }
}
