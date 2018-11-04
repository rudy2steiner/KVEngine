package com.alibabacloud.polar_race.engine.kv;

import com.alibabacloud.polar_race.engine.common.utils.Files;
import com.alibabacloud.polar_race.engine.kv.index.WalIndexLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;

public class DoubleBuffer {
    private final static Logger logger= LoggerFactory.getLogger(DoubleBuffer.class);
    private  volatile boolean  readable;
    private  volatile  boolean  writable;
    private  int size;
    private volatile ByteBuffer readBuffer;
    private volatile ByteBuffer writeBuffer;
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
     * 读写buffer 交换
     **/
    public synchronized void swap(boolean write2read) throws Exception{
        //还没读完，不可以交换
        while (readable) {
            logger.info("wait readable false");
            Thread.sleep(5);
        }
        ByteBuffer buf=readBuffer;
        readBuffer = writeBuffer;
        writeBuffer = buf;
        readable=true;
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

    /**
     *
     **/
    public void release(){
       if(readBuffer.isDirect()){
           ((DirectBuffer)readBuffer).cleaner();
           ((DirectBuffer)writeBuffer).cleaner();
       }else{
           readBuffer=null;
           writeBuffer=null;
       }
    }

}
