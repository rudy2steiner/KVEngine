package com.alibabacloud.polar_race.engine.kv.index;
import com.alibabacloud.polar_race.engine.common.Lifecycle;
import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.common.io.IOHandler;
import com.alibabacloud.polar_race.engine.kv.LogFileService;
import com.alibabacloud.polar_race.engine.kv.buffer.BufferHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
public class IndexLogReader implements Lifecycle {
    private final static Logger logger= LoggerFactory.getLogger(IndexLogReader.class);
    private String logDir;
    private LogFileService logFileService;
    private List<Long>  logFiles;
    private ExecutorService indexHashService;
    public IndexLogReader(String logDir, LogFileService logFileService){
           this.logDir=logDir;
           this.logFileService=logFileService;
    }

    @Override
    public void start() throws Exception {
          this.logFiles=logFileService.allLogFiles();
          this.indexHashService= Executors.newFixedThreadPool(StoreConfig.HASH_CONCURRENCY);
    }

    @Override
    public void close() throws Exception {
         if(!indexHashService.isShutdown()){
             if(!indexHashService.isTerminated()){
                 if(indexHashService.awaitTermination(1, TimeUnit.SECONDS)){
                     logger.info("tasks finished,and shutdown");
                 }else{
                     logger.info("tasks close timeout,and shutdown");
                     indexHashService.shutdownNow();
                 }
             }
         }
    }

    public void iterate(IndexVisitor visitor) throws Exception{
        int concurreny=StoreConfig.HASH_CONCURRENCY;
        int perThreadFiles=1;
        int mod=0;
        int logCount=logFiles.size();
        if(logCount==0) return;
        if(logCount>concurreny){
            perThreadFiles=logFiles.size()/concurreny;
            mod=logFiles.size()%concurreny;
        }else{
            concurreny=logFiles.size();
        }
        int start;
        int end=0;
        Runnable task;
        for(int i=0;i<concurreny;i++){
            start=end;
            if(i<mod){
                end+=1;
            }
             end+=perThreadFiles;
             task= new Reader(logFiles,start,end,visitor);
             logger.info(String.format("add task,start %d,end %d",start,end));
             indexHashService.submit(task);
        }
        indexHashService.shutdown();
        if(indexHashService.awaitTermination(50, TimeUnit.SECONDS)){
            logger.info("tasks finished,and shutdown");
        }else{
            logger.info("tasks timeout,and shutdown");
            indexHashService.shutdownNow();
        }
    }
    public class Reader implements Runnable{
        private List<Long> files;
        private int start;
        private int end;
        private IOHandler handler;
        private int readOffset;
        private ByteBuffer buffer;
        private IndexVisitor visitor;
        /**
         * @param start include
         * @param end  exclude
         *
         **/
        public Reader(List<Long> files,int start,int end,IndexVisitor visitor){
           this.files=files;
           this.start=start;
           this.end=end;
           this.readOffset= StoreConfig.SEGMENT_LOG_FILE_SIZE-logFileService.tailerAndIndexSize();
           this.buffer=ByteBuffer.allocateDirect(logFileService.tailerAndIndexSize());
           this.visitor=visitor;
        }
        @Override
        public void run() {
            try {
                for (int i = start; i < end; i++) {
                    //logger.info("start process wal "+files.get(i));
                    handler = logFileService.ioHandler(files.get(i) + StoreConfig.LOG_FILE_SUFFIX);
                    handler.position(readOffset);
                    buffer.clear();
                    handler.read(buffer);
                    readPost(buffer,files.get(i));
                    //logger.info("finish process wal "+files.get(i));
                }
            }catch (Exception e){
                logger.info("read exception and stop",e);
            }
            close();
        }
        public void readPost(ByteBuffer buffer, long fileNO) throws Exception{
                buffer.flip();
                if(buffer.hasRemaining()) {
                    byte version = buffer.get();
                    int size = buffer.getInt();
                    logger.info(String.format("version %d,index buffer size %d,file %d", (int) version, size,fileNO));
                    buffer.position(StoreConfig.VALUE_INDEX_RECORD_SIZE);
                    buffer.limit(size);
                    visitor.visit(buffer);
                }else {
                    logger.info(String.format("buf no remaining "+fileNO));
                    throw new IllegalArgumentException("buf no remaining");
                }
        }

        public void close(){
            BufferHandler.release(buffer);
            buffer=null;
        }
    }
}
