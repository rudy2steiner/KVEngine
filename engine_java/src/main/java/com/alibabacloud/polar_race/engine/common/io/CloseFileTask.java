package com.alibabacloud.polar_race.engine.common.io;

import com.alibabacloud.polar_race.engine.common.utils.Memory;
import com.alibabacloud.polar_race.engine.kv.file.LogFileServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.alibabacloud.polar_race.engine.kv.file.LogFileServiceImpl.closeHandlerCounter;

public class CloseFileTask implements Runnable{
    private final static Logger logger= LoggerFactory.getLogger(LogFileServiceImpl.class);
        private IOHandler handler;
        public CloseFileTask(IOHandler handler){
            this.handler=handler;
        }
        @Override
        public void run() {
            try {
                long start=System.currentTimeMillis();
                int closed=closeHandlerCounter.incrementAndGet();
                if(closed%100000==0){
                    logger.info(String.format("closed %d io handler,and close this %s now,time %d ms",closed,handler.name(),System.currentTimeMillis()-start));
//                    handler.closeFileChannel(true);
                    logger.info(Memory.memory().toString());
                }
                handler.closeFileChannel(true);
                // don't cache
                handler.dontNeed(0,0);
            }catch (IOException e){
                logger.info(String.format("asyncClose %s exception,ignore",handler.name()),e);
            }
        }
}
