package com.alibabacloud.polar_race.engin;

import com.alibabacloud.polar_race.engine.common.utils.Memory;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryTest {
    private final static Logger logger= LoggerFactory.getLogger(MemoryTest.class);
    @Test
    public void flushPageCache(){

        logger.info(Memory.memory().toString());

        Memory.sync();
        logger.info(Memory.execute("whoami"));
        logger.info(Memory.execute("ls -l /"));
        logger.info(Memory.execute("ls -l /proc/sys/vm/drop_caches"));
        logger.info("flushed page cache");
        logger.info(Memory.memory().toString());
        //Memory.jvmHeap();
//        Memory.parseMemory();
    }


}
