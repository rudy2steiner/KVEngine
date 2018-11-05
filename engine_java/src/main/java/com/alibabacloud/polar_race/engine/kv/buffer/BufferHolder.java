package com.alibabacloud.polar_race.engine.kv.buffer;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class BufferHolder implements RecyclableBuffer {
    private AtomicInteger holder=new AtomicInteger(0);
    private ByteBuffer buffer;
    public BufferHolder(ByteBuffer buffer) {
        this.buffer=buffer;
    }
    public ByteBuffer value(){
        return buffer;
    }
    @Override
    public int retain() {
           return    holder.incrementAndGet();
    }

    @Override
    public void free() {
         LogBufferAllocator.release(buffer);
         buffer=null;
    }

    @Override
    public boolean available() {
        if(holder.get()==0) return true;
        return false;
    }

    @Override
    public int release() {
            return holder.decrementAndGet();
    }
}
