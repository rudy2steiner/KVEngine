package com.alibabacloud.polar_race.engine.kv.index;

import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.common.io.IOHandler;
import com.alibabacloud.polar_race.engine.kv.buffer.LogBufferAllocator;
import com.alibabacloud.polar_race.engine.kv.cache.IOHandlerLRUCache;
import com.alibabacloud.polar_race.engine.kv.file.LogFileService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;

public class IndexWalLogReader implements Runnable{
    private final static Logger logger= LoggerFactory.getLogger(IndexWalLogReader.class);
    private LogFileService logFileService;
    private List<Long> files;
    private int start;
    private int end;
    private IOHandler handler;
    private int readOffset;
    private ByteBuffer buffer;
    private IOHandlerLRUCache logHandlerCache;
    private IndexVisitor visitor;
    /**
     * @param start include
     * @param end  exclude
     *  256 kb direct buffer
     **/
    public IndexWalLogReader(LogFileService logFileService, IOHandlerLRUCache logHandlerCache, List<Long> files, int start, int end, IndexVisitor visitor){
        this.files=files;
       this.start=start;
       this.end=end;
       this.logFileService=logFileService;
       this.logHandlerCache=logHandlerCache;
       this.readOffset= StoreConfig.SEGMENT_LOG_FILE_SIZE-logFileService.tailerAndIndexSize();
       this.buffer=ByteBuffer.allocate(logFileService.tailerAndIndexSize());
       this.visitor=visitor;
    }
    @Override
    public void run() {
        try {
            for (int i = start; i < end; i++) {
                //logger.info("start process wal "+files.get(i));
                handler = logHandlerCache.get(files.get(i));//logFileService.ioHandler(files.get(i) + StoreConfig.LOG_FILE_SUFFIX);
                if(handler==null) throw new IllegalArgumentException("put handler not found");
                //handler.position(readOffset);
                buffer.clear();
                handler.read(readOffset,buffer);
                readPost(buffer,files.get(i));
                //logger.info("finish process wal "+files.get(i));
                //handler.closeFileChannel(false);
                //NativeIO.posixFadvise(handler.fileDescriptor(),0,readOffset,NativeIO.POSIX_FADV_NORMAL);
                //handler.dontNeed(readOffset,logFileService.tailerAndIndexSize());
                //logFileService.asyncCloseFileChannel(handler);
            }
            visitor.onFinish();
        }catch (Exception e){
            logger.info("read exception and stop",e);
            visitor.onException();
        }finally {
            close();
        }
    }
    public void readPost(ByteBuffer buffer, long fileNO) throws Exception{
            buffer.flip();
            if(buffer.hasRemaining()) {
                byte version = buffer.get();
                int size = buffer.getInt();
                if(size<2000)
                    logger.info(String.format("version %d,index buffer expected Size %d,file %d", (int) version, size,fileNO));
                buffer.position(StoreConfig.VALUE_INDEX_RECORD_SIZE);
                buffer.limit(size);
                visitor.visit(buffer);
            }else {
                logger.info(String.format("buf no remaining ,ignore"+fileNO));
            }
    }

    public void close(){
        logger.info(String.format("read %d index put file",end-start));
        LogBufferAllocator.release(buffer);
        buffer=null;
    }
}
