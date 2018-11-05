package com.alibabacloud.polar_race.engine.kv.buffer;

import com.alibabacloud.polar_race.engine.kv.LogFileService;
import sun.nio.ch.DirectBuffer;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class LogBufferAllocator implements BufferSizeAware, Closeable {

    /**
     *  记录已分配的内存大小
     **/
    private AtomicInteger bufferSizeAllocated=new AtomicInteger(0);
    private int maxDirectBufferBucks;
    private int maxHeapBufferBucks;
    private AtomicInteger allocatedDirectBucks=new AtomicInteger(0);
    private BlockingQueue<BufferHolder> directCache;
    private BlockingQueue<BufferHolder> heapCache;
    private AtomicInteger allocatedHeapBucks=new AtomicInteger(0);

    private LogFileService logFileService;
    public LogBufferAllocator(LogFileService logFileService, int maxDirectBufferBucks, int maxHeapBufferBucks){
            this.maxDirectBufferBucks=maxDirectBufferBucks;
            this.maxHeapBufferBucks=maxHeapBufferBucks;
            this.directCache=new ArrayBlockingQueue(maxDirectBufferBucks);
            this.heapCache=new ArrayBlockingQueue(maxHeapBufferBucks);
            this.logFileService=logFileService;
    }
    public static void release(ByteBuffer buffer){
        if(buffer.isDirect()){
            ((DirectBuffer)buffer).cleaner();
        }
    }

    /**
     * @return direct buffer or null
     **/
    public BufferHolder allocatDirect(){
          BufferHolder holder= directCache.poll();
          if(holder==null&&allocatedDirectBucks.getAndIncrement()<=maxDirectBufferBucks){
                holder=new BufferHolder(ByteBuffer.allocateDirect(logFileService.logWritableSize()));
                onAdd(logFileService.logWritableSize());
          }
          return holder;
    }

    /**
     * block until put finish
     *
     **/
    public void rebackDirect(BufferHolder holder) throws InterruptedException{
            directCache.put(holder);
    }


    /**
     * @return heap buffer or null
     **/
    public BufferHolder allocateHeap(){
        BufferHolder holder= heapCache.poll();
        if(holder==null&&allocatedHeapBucks.getAndIncrement()<=maxHeapBufferBucks){
            holder=new BufferHolder(ByteBuffer.allocate(logFileService.logWritableSize()));
            onAdd(logFileService.logWritableSize());
        }
        return holder;
    }


    /**
     * block until put finish
     *
     **/
    public void rebackHeap(BufferHolder holder) throws InterruptedException{
        if(!needGC(holder))
            heapCache.put(holder);
    }

    /**
     * @return  是否 release
     **/
    public boolean needGC(BufferHolder holder){
        // to do gc

        return false;
    }



    @Override
    public void onAdd(int size) {
         bufferSizeAllocated.getAndAdd(size);
    }

    @Override
    public void onRelease(int size) {
        bufferSizeAllocated.getAndAdd(-size);
    }

    @Override
    public void close() throws IOException {
        heapCache.clear();
        BufferHolder holder;
        do {
            holder=directCache.poll();
            release(holder.value());
        }while (holder==null);
    }
}
