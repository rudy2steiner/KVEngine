package com.alibabacloud.polar_race.engin;

import com.alibabacloud.polar_race.engine.kv.WALogger;
import org.junit.Test;

public class IndexTest {
    private String root="/export/wal000/";
    @Test
    public void KeyIndexIterate() throws Exception{
        WALogger logger=new WALogger(root);
        logger.startHashEngine();

    }

    @Test
    public void  testMax(){
        System.out.println(Math.max(2,3));
    }
    @Test
    public void IndexIterate() throws Exception{

        WALogger logger=new WALogger(root);
        logger.startIndexEngine();

    }
}
