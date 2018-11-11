package com.alibabacloud.polar_race.engin;

import com.alibabacloud.polar_race.engine.common.utils.Memory;
import com.alibabacloud.polar_race.engine.common.utils.MemoryInfo;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryTest {
    private final static Logger logger= LoggerFactory.getLogger(MemoryTest.class);
    @Test
    public void flushPageCache(){

        logger.info(Memory.memory().toString());
        Memory.sync();
        logger.info(Memory.memory().toString());
    }
}
