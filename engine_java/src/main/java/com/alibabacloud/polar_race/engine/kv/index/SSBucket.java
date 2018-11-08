package com.alibabacloud.polar_race.engine.kv.index;

import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.common.utils.Files;
import com.alibabacloud.polar_race.engine.kv.buffer.DoubleBuffer;
import com.alibabacloud.polar_race.engine.kv.event.IndexLogEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class SSBucket {
    private int id;
    private int indexSize= StoreConfig.VALUE_INDEX_RECORD_SIZE;
    private int bufferSize;
    private volatile  ByteBuffer buffer;
    private volatile DoubleBuffer doubleBuffer;
    private AtomicInteger index=new AtomicInteger(0);
    private WalIndexLogger walLogger;
    private final static Logger logger= LoggerFactory.getLogger(SSBucket.class);
    private AtomicInteger writeCounter=new AtomicInteger(0);
    private int maxWriteCounter;
    public void SSBucket(){
    }
    public SSBucket(int id,int bufferSize){
        this(id,ByteBuffer.allocateDirect(Files.tableSizeFor(bufferSize)));
    }
    public SSBucket(int id,ByteBuffer buffer){
         this.id=id;
         this.buffer=buffer;
         this.bufferSize=buffer.capacity();
    }
    public SSBucket(int id, DoubleBuffer doubleBUffer){
         this(id,doubleBUffer.get(false));
         this.doubleBuffer =doubleBUffer;
    }
    public SSBucket(int id, DoubleBuffer doubleBUffer,WalIndexLogger logger){
        this(id,doubleBUffer.get(false));
        this.doubleBuffer =doubleBUffer;
        this.walLogger =logger;
        this.maxWriteCounter=doubleBUffer.maxWriteCount();
    }

    public void put(ByteBuffer index,int offset) throws Exception{
        if(buffer.remaining()==0){
            logger.info("bug");
        }
        ByteBuffer buf = buffer.slice();
        buf.position(offset);
        buf.put(index);
        if(writeCounter.incrementAndGet()%maxWriteCounter==0){
            swap(maxWriteCounter*StoreConfig.VALUE_INDEX_RECORD_SIZE);
        }
    }

    /**
     * 获取下一个写偏移量
     * @return -1,try again
     *
     */
    public int getNextOffset() throws Exception{
              int offset=index.getAndAdd(indexSize);
              if(offset+indexSize<=bufferSize) {
                  return offset;
              }
              //swap(offset);
              return -1;
    }

    public synchronized void swap(int position) throws Exception{
          //if(position<=bufferSize)
            flushBuffer(position);
    }
    /**
     *
     * 将可读buffer 写出
     *
     **/
    public void notifyRead() throws Exception{
         IndexLogEvent indexLogEvent=new IndexLogEvent(doubleBuffer);
         indexLogEvent.setTxId(id);
         walLogger.put(indexLogEvent);
    }


    public synchronized void flushBuffer(int position) throws Exception{
        if(position<=buffer.limit()){
            ByteBuffer slice=doubleBuffer.slice(true);
            slice.position(position);
            slice.flip();
            buffer= doubleBuffer.get(false);
            buffer.clear();
            index.set(0);
            // 可读
            doubleBuffer.state(true,true);
            notifyRead();
        }else{
            //logger.info("excel");
        }
    }


    public void close() throws Exception{
        if(index.get()>0)
            flushBuffer(index.get());
        this.buffer=null;
        this.doubleBuffer.release();
    }



}
