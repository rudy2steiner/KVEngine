package com.alibabacloud.polar_race.engin;


import com.alibabacloud.polar_race.engine.common.utils.Memory;
import net.smacke.jaydio.DirectRandomAccessFile;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.misc.Unsafe;

import java.io.File;
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
        int bufferSize =
                // use 8 MiB buffers by default
               8*1024*1024;

        byte[] buf = new byte[bufferSize];
        String[] args={"/export/kv_test.log.2018-12-02.log","/export/log_copy.log"};
        DirectRandomAccessFile fin =
                new DirectRandomAccessFile(new File(args[0]), "r", bufferSize);

        DirectRandomAccessFile fout =
                new DirectRandomAccessFile(new File(args[1]), "rw", bufferSize);

        while (fin.getFilePointer() < fin.length()) {
            int remaining = (int)Math.min(bufferSize, fin.length()-fin.getFilePointer());
            fin.read(buf,0,remaining);
            fout.write(buf,0,remaining);
        }

        fin.close();
        fout.close();


    }


}
