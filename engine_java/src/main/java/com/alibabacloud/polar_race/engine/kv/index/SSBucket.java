package com.alibabacloud.polar_race.engine.kv.index;

import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.common.utils.Files;
import com.alibabacloud.polar_race.engine.kv.DoubleBuffer;
import com.alibabacloud.polar_race.engine.kv.event.IndexLogEvent;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class SSBucket {
    private int id;
    private int indexSize= StoreConfig.VALUE_INDEX_RECORD_SIZE;
    private int bufferSize;
    private ByteBuffer buffer;
    private DoubleBuffer doubleBuffer;
    private AtomicInteger index=new AtomicInteger(0);
    private WalIndexLogger logger;
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
        this.logger=logger;
    }

    public void put(ByteBuffer index,int offset) {
        ByteBuffer buf = buffer.slice();
        buf.position(offset);
        buf.put(index);
    }

    /**
     * 获取下一个写偏移量
     * @return -1,try again
     *
     */
    public int getNextOffset() throws Exception{
              int offset=index.getAndAdd(indexSize);
              if(offset<bufferSize) return offset;
              swap();
              return -1;
    }

    public synchronized void swap() throws Exception{
           if(index.get()>=bufferSize){
               buffer.flip();
               doubleBuffer.swap(true);
               buffer= doubleBuffer.get(false);
               buffer.clear();
               index.set(0);
               // 可读
               doubleBuffer.state(true,true);
               notifyRead();
           }
    }
    /**
     *
     * 将可读buffer 写出
     *
     **/
    public void notifyRead() throws Exception{
         IndexLogEvent indexLogEvent=new IndexLogEvent(doubleBuffer);
         indexLogEvent.setTxId(id);
         logger.put(indexLogEvent);
    }




}
