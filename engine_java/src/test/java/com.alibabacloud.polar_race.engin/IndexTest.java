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
    public void IndexIterate() throws Exception{

        WALogger logger=new WALogger(root);
        logger.startIndexEngine();

    }
}
