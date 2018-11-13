package com.alibabacloud.polar_race.engin;


import com.alibabacloud.polar_race.engine.common.utils.Memory;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.misc.Unsafe;

import java.io.IOException;

@Ignore
public class MemoryTest {
    private final static Logger logger= LoggerFactory.getLogger(MemoryTest.class);
    @Test
    public void flushPageCache(){

        logger.info(Memory.memory().toString());
        Memory.sync();
        logger.info(Memory.execute("whoami"));
        logger.info(Memory.execute("ls -l /"));
        logger.info(Memory.execute("ls -l /proc/sys/vm/drop_caches"));
        logger.info(Memory.execute("cat /proc/sys/vm/drop_caches"));
        logger.info("flushed page cache");
        logger.info(Memory.memory().toString());
        Unsafe unsafe;
    }

    @Test
    public void nativeIo() throws IOException {
        //NativeIO io=new NativeIO();



    }


}
