package com.alibabacloud.polar_race.engine.kv.buffer;

import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.common.utils.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DoubleBuffer {
    private final static Logger logger= LoggerFactory.getLogger(DoubleBuffer.class);
    private  volatile boolean  readable;
    private  volatile  boolean  writable;
    private  int size;
    private volatile ByteBuffer readBuffer;
    private volatile ByteBuffer writeBuffer;
    private ReadWriteLock lock=new ReentrantReadWriteLock();
    public DoubleBuffer(int bufferSize, boolean direct){
        this.size= Files.tableSizeFor(bufferSize);
        if(direct){
            readBuffer=ByteBuffer.allocateDirect(size);
            writeBuffer=ByteBuffer.allocateDirect(size);
        }else {
            readBuffer=ByteBuffer.allocate(size);
            writeBuffer=ByteBuffer.allocate(size);
        }
    }

    /**
     * 读写buffer 交换,
     * @return the sliced write buffer
     **/
    public  ByteBuffer slice(boolean write2read) throws Exception{
        //还没读完，不可以交换
        lock.writeLock().lock();
        while (readable) {
            logger.info("wait readable false");
            Thread.sleep(5);
        }
        ByteBuffer buf=readBuffer;
        readBuffer = writeBuffer;
        writeBuffer = buf;
        readable=true;
        lock.writeLock().unlock();
        return readBuffer;
    }

    public ByteBuffer get(boolean read){
        if(read)
            return readBuffer;
        else return writeBuffer;
    }
    /**
     * 改变buffer 的状态
     **/
    public synchronized void state(boolean read,boolean state){
        if(read)
            readable=state;
        else
            writable=state;
    }

    public int maxWriteCount(){
        return size/StoreConfig.VALUE_INDEX_RECORD_SIZE;
    }

    /**
     *
     **/
    public void release(){
       if(readBuffer.isDirect()){
           ((DirectBuffer)readBuffer).cleaner().clean();
           ((DirectBuffer)writeBuffer).cleaner().clean();
       }else{
           readBuffer=null;
           writeBuffer=null;
       }
    }

}
