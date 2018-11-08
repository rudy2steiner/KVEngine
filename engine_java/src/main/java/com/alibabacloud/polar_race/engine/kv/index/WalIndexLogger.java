package com.alibabacloud.polar_race.engine.kv.index;

import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.common.io.IOHandler;
import com.alibabacloud.polar_race.engine.kv.file.LogFileService;
import com.alibabacloud.polar_race.engine.kv.file.LogFileServiceImpl;
import com.alibabacloud.polar_race.engine.kv.event.IndexLogEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class WalIndexLogger {
    private final static Logger logger= LoggerFactory.getLogger(WalIndexLogger.class);
    private int consumeTimeout=100;
    private String dir;
    private int buckSize;
    private LogFileService fileService;
    private Map<Long, IOHandler> handlerMap;
    public WalIndexLogger(String dir,int buckSize){
        this.dir=dir;
        this.buckSize=buckSize;
        this.fileService=new LogFileServiceImpl(dir);
        this.handlerMap=new HashMap<>(buckSize*2);

    }

    public void start() throws Exception{
        IOHandler handler;
        for(int i=0;i<buckSize;i++){
            handler=fileService.ioHandler(String.valueOf(i)+ StoreConfig.LOG_INDEX_FILE_SUFFIX);
            if(handler.length()>0){
                // move to end
                handler.position(handler.length());
            }
            handlerMap.put((long)i,handler);
        }

    }
    /**
     * wait until put success
     **/
    public void put(IndexLogEvent event) throws IOException{
        IOHandler handler=handlerMap.get(event.txId());
        if(handler!=null) {
            handler.append(event.value().get(true));
            event.value().state(true,false);
        }else{
            logger.info("index log thread handler not init");
            throw new IllegalArgumentException("io handler not found");
        }
    }
    /**
     * stop
     *
     **/
    public void stop() throws InterruptedException{
//         if(writer!=null){
//             writer.stop(true);
//         }

    }



}
