package com.alibabacloud.polar_race.engine.kv.wal;


import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.common.io.IOHandler;
import com.alibabacloud.polar_race.engine.kv.file.LogFileService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * read last log and index write index at the tail
 *
 **/
public class WalLogParser implements KVParser {
    private final static Logger logger= LoggerFactory.getLogger(WalLogParser.class);
    private IOHandler handler;
    private String fileName;
    private LogFileService logFileService;
    public WalLogParser(LogFileService logFileService,String fileName) throws IOException {
        this.fileName=fileName;
        this.logFileService=logFileService;
        // no buffer cache
        this.handler=logFileService.ioHandler(fileName);
        if(handler.length()>logFileService.logWritableSize()){
            handler.truncate(logFileService.logWritableSize());
        }
    }

    @Override
    public void parse(AbstractVisitor visitor, ByteBuffer to, ByteBuffer from) {

    }

    @Override
    public IOHandler doRecover(AbstractVisitor visitor, ByteBuffer to, ByteBuffer from) throws IOException {
        int capacity=to.capacity();
        int remain=0;
        int i=0;
        ByteBuffer indexBuffer=ByteBuffer.allocate(logFileService.tailerAndIndexSize());
        indexBuffer.put(StoreConfig.VERSION);
        indexBuffer.position(StoreConfig.VALUE_INDEX_RECORD_SIZE);
        long fileStartPosition=Long.valueOf(handler.name());
        logger.info(String.format("recovering last file %s,expectedSize %d",handler.name(),handler.length()));
        int  maxValueLength=0;
        do{
            i++;
            handler.position(maxValueLength);
            handler.read(to);
            to.flip();
            remain=to.remaining();
            int position=extractIndex(to,indexBuffer,fileStartPosition+maxValueLength);
            maxValueLength+=position;
            if(to.hasRemaining()){
                to.compact();
            }else to.clear();
        }while (remain==capacity);
        logTailerAndIndex(indexBuffer,maxValueLength);
        // no need anymore
        logFileService.asyncCloseFileChannel(handler);
        return handler;
    }


    /**
     *
     */
    public int extractIndex(ByteBuffer buffer,ByteBuffer indexBuffer,long baseOffset){
        int len=0;
        long offset;
        while(buffer.remaining()>=2){
            buffer.mark();
            offset=buffer.position();
            len=buffer.getShort();
            if(len==0){
                buffer.position(buffer.limit());
                // end of file
                return buffer.limit();
            }
            if(buffer.remaining()<len){
                // 回退
                buffer.reset();
                return buffer.position();
            }
            long key=buffer.getLong();
                 offset+=baseOffset;
            // skip value len
            buffer.position(buffer.position()+len-StoreConfig.KEY_SIZE);
            indexBuffer.putLong(key);
            indexBuffer.putLong(offset);
        }
        return  buffer.position();
    }

    /**
     *  log value end mark should append
     *
     **/
    public void logTailerAndIndex(ByteBuffer  indexBuffer,int maxValueLength) throws IOException{
           if(handler.length()!=maxValueLength){
               // 随机kill 可能写一半，丢弃
               logger.info(" last log file, partial write");
           }
        long remain= logFileService.logWritableSize()-handler.length();
           if(remain>0) {
               MultiTypeEventHandler.EMPTY_BUFFER.clear();
               if (remain >= StoreConfig.LOG_KV_RECORD_LEAST_LEN) {
                   MultiTypeEventHandler.EMPTY_BUFFER.putShort((short) 0);
                   // fill empty
                   MultiTypeEventHandler.EMPTY_BUFFER.position(Math.min((int) remain,StoreConfig.K4_SIZE));
               } else {
                   MultiTypeEventHandler.EMPTY_BUFFER.putChar('#');
               }
               MultiTypeEventHandler.EMPTY_BUFFER.flip();
               handler.position(handler.length());
               handler.append(MultiTypeEventHandler.EMPTY_BUFFER);
           }
        int size=indexBuffer.position();
        // store tail and value index real expectedSize
        indexBuffer.position(1);
        indexBuffer.putInt(size);
        indexBuffer.position(indexBuffer.capacity());
        indexBuffer.flip();
        // fill empty
        handler.position(StoreConfig.SEGMENT_LOG_FILE_SIZE-indexBuffer.capacity());
        handler.append(indexBuffer);
    }
}
