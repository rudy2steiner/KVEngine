package com.alibabacloud.polar_race.engine.kv.wal;


import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.common.io.IOHandler;
import com.alibabacloud.polar_race.engine.kv.file.LogFileService;


import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * read last log and index write index at the tail
 *
 **/
public class WalLogParser implements KVParser {
    private IOHandler handler;
    private String fileName;
    private LogFileService logFileService;
    public WalLogParser(LogFileService logFileService,String fileName) throws IOException {
        this.fileName=fileName;
        this.logFileService=logFileService;
        // no buffer cache
        this.handler=logFileService.ioHandler(fileName);
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
        indexBuffer.put(StoreConfig.verison);
        indexBuffer.position(StoreConfig.VALUE_INDEX_RECORD_SIZE);
        long fileStartPosition=Long.valueOf(handler.name());
        int  maxValueLength=0;
        do{
            i++;
            handler.read(to);
            to.flip();
            remain=to.remaining();
            int position=extractIndex(to,indexBuffer,fileStartPosition+maxValueLength);
            maxValueLength+=position;
            if(to.hasRemaining()){
                from.put(to);
                to.clear();
                from.flip();
                to.put(from);
                from.clear();
            }
        }while (remain==capacity);
        logTailerAndIndex(indexBuffer,maxValueLength);
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
               throw new IOException("value length wrong");
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
        // store tail and value index real size
        indexBuffer.position(1);
        indexBuffer.putInt(size);
        indexBuffer.position(indexBuffer.capacity());
        indexBuffer.flip();
        // fill empty
        handler.position(StoreConfig.SEGMENT_LOG_FILE_SIZE-indexBuffer.capacity());
        handler.append(indexBuffer);
    }
}
