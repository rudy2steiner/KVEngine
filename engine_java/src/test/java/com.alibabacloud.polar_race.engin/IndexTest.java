package com.alibabacloud.polar_race.engin;

import com.alibabacloud.polar_race.engine.common.utils.Files;
import com.alibabacloud.polar_race.engine.kv.wal.WALogger;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.BitSet;

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
        WALogger logger=new WALogger(root);
        logger.startAsyncHashTask();

    }

    @Test
    public void  testMax(){
        System.out.println(Math.max(2,3));
    }
    @Test
    public void IndexIterate() throws Exception{

        WALogger logger=new WALogger(root);
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
}
