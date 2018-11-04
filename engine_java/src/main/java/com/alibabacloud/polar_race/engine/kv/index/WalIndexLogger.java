package com.alibabacloud.polar_race.engine.kv.index;

import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.common.io.IOHandler;
import com.alibabacloud.polar_race.engine.kv.LogFileService;
import com.alibabacloud.polar_race.engine.kv.LogFileServiceImpl;
import com.alibabacloud.polar_race.engine.kv.event.IndexLogEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class WalIndexLogger {
    private final static Logger logger= LoggerFactory.getLogger(WalIndexLogger.class);
    private BlockingQueue<IndexLogEvent> buckTrunck;
    private int consumeTimeout=100;
    private String dir;
    private int buckSize;
    private LogFileService fileService;
    private Map<Long, IOHandler> handlerMap;
    public WalIndexLogger(String dir,int buckSize,int queueSize){
        this.dir=dir;
        this.buckSize=buckSize;
        buckTrunck=new ArrayBlockingQueue<>(queueSize);
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
//    public class LogThread implements Runnable{
//        private boolean running;
//        private IndexLogEvent processing;
//        private Map<Long, IOHandler> handlerMap;
//        private IOHandler handler;
//        public LogThread(int buckSize){
//                handlerMap=new HashMap<>(buckSize);
//        }
//        public void start()throws IOException {
//            this.running=true;
//            for(int i=0;i<buckSize;i++){
//                handler=fileService.ioHandler(String.valueOf(i)+ StoreConfig.LOG_INDEX_FILE_SUFFIX);
//                if(handler.length()>0){
//                    // move to end
//                    handler.position(handler.length());
//                }
//                handlerMap.put((long)i,handler);
//            }
//        }
//
//        public synchronized void stop(boolean gracefully) throws InterruptedException{
//            if(running){
//                running=false;
//                wait();
//            }
//        }
//
//        @Override
//        public void run() {
//            try {
//                while(true) {
//                    processing = buckTrunck.poll(consumeTimeout, TimeUnit.MILLISECONDS);
//                    if (processing == null) {
//                        if(!onIdle()) break;
//                    }
//                    handler=handlerMap.get(processing.txId());
//                    if(handler!=null) {
//                        handler.append(processing.value().get(true));
//                        processing.value().state(true,false);
//                    }else{
//                        logger.info("index log thread handler not init");
//                        break;
//                    }
//                }
//            }catch (InterruptedException e){
//                logger.info("index log thread interrupted",e);
//            }catch (IOException e){
//                logger.info("index log thread io exception",e);
//            }
//        }
//        /**
//         * @return false  stop the thread
//         **/
//        public boolean onIdle(){
//               if(!running){
//                   notify();
//                   return false;
//               }
////               try {
////                   Thread.sleep(100);
////               }catch (InterruptedException e){
////                   e.printStackTrace();
////               }
//               return true;
//
//        }
//    }


}
