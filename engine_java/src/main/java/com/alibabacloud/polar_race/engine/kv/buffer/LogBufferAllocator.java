package com.alibabacloud.polar_race.engine.kv.buffer;

import com.alibabacloud.polar_race.engine.common.utils.Null;
import com.alibabacloud.polar_race.engine.kv.file.LogFileService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.DirectBuffer;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class LogBufferAllocator implements BufferSizeAware, Closeable {
    private final static Logger logger= LoggerFactory.getLogger(LogBufferAllocator.class);
    /**
     *  记录已分配的内存大小
     **/
    private AtomicInteger bufferSizeAllocated=new AtomicInteger(0);
    private volatile int maxDirectBufferBucks;
    private volatile int maxHeapBufferBucks;
    private AtomicInteger allocatedDirectBucks=new AtomicInteger(0);
    private BlockingQueue<BufferHolder> directCache;
    private BlockingQueue<BufferHolder> heapCache;
    private AtomicInteger allocatedHeapBucks=new AtomicInteger(0);
    private AtomicInteger maxDirectBufferSize=new AtomicInteger(0);
    private AtomicInteger maxHeapBufferSize=new AtomicInteger(0);
    private AtomicInteger allocatedDirectBufferSize=new AtomicInteger(0);
    private AtomicInteger allocatedHeapBufferSize=new AtomicInteger(0);
    private LogFileService logFileService;
    private AtomicInteger allocateCounter=new AtomicInteger(0);
    public LogBufferAllocator(LogFileService logFileService, int maxDirectLogCacheBufferBucks, int maxHeapLogCacheBufferBucks,int maxDirectBuffeSize,int maxHeapBufferSize){
            this.maxDirectBufferBucks=maxDirectLogCacheBufferBucks;
            this.maxHeapBufferBucks=maxHeapLogCacheBufferBucks;
            this.directCache=new ArrayBlockingQueue(maxDirectLogCacheBufferBucks);
            this.heapCache=new ArrayBlockingQueue(maxHeapLogCacheBufferBucks);
            this.maxDirectBufferSize.set(maxDirectBuffeSize);
            this.maxHeapBufferSize.set(maxHeapBufferSize);
            this.logFileService=logFileService;
    }
    public static void release(ByteBuffer buffer){
        if(buffer!=null&&buffer.isDirect()){
            ((DirectBuffer)buffer).cleaner().clean();
        }
    }

    /**
     * @return direct buffer or null
     **/
    public BufferHolder allocateDirectLogCache(){
          BufferHolder holder= directCache.poll();
          if(Null.isEmpty(holder)){
              if( allocatedDirectBucks.getAndIncrement()<maxDirectBufferBucks) {
                  try {
                      holder = new BufferHolder(allocate(logFileService.logWritableSize(), true));
                  }finally {
                      if(Null.isEmpty(holder)) allocatedDirectBucks.decrementAndGet();
                  }
              }else allocatedDirectBucks.decrementAndGet();

          }
          return holder;
    }

    /**
     * block until put finish
     *
     **/
    public void rebackDirectLogCache(BufferHolder holder) throws InterruptedException{
            directCache.put(holder);
    }


    /**
     * @return heap buffer or null
     **/
    public BufferHolder allocateHeapLogCache(){
        BufferHolder holder= heapCache.poll();
        if(Null.isEmpty(holder)){
            if(allocatedHeapBucks.getAndIncrement()<maxHeapBufferBucks) {
                try {
                    holder = new BufferHolder(allocate(logFileService.logWritableSize(), false));
                }finally {
                    if(holder==null) allocatedHeapBucks.decrementAndGet();
                }
            }else {
                allocatedHeapBucks.decrementAndGet();
            }
            //onAdd(logFileService.logWritableSize(),false);
        }
        return holder;
    }


    /**
     * block until put finish
     *
     **/
    public void rebackHeapLogCache(BufferHolder holder) throws InterruptedException{
        if(!needGC(holder))
            heapCache.put(holder);
    }


    /**
     *  按需分配缓存
     **/
    public ByteBuffer allocate(int size,boolean direct){
            if(direct){
                if(onAdd(size,direct))
                    return ByteBuffer.allocateDirect(size);
            }else{
                if(onAdd(size,direct)){
                    return ByteBuffer.allocate(size);
                }
            }
            throw new IllegalArgumentException("allocate buffer failed");
    }


    /**
     * @return  是否 release
     **/
    public boolean needGC(BufferHolder holder){
        // to do gc

        return false;
    }


    @Override
    public boolean onAdd(int size,boolean direct) {
        if(direct){
            if( allocatedDirectBufferSize.addAndGet(size)>maxDirectBufferSize.get()) {
                allocatedDirectBufferSize.addAndGet(-size);
                return false;
            }
        }else {
            if(allocatedHeapBufferSize.addAndGet(size)>maxHeapBufferSize.get()) {
                allocatedHeapBufferSize.addAndGet(-size);
                return false;
            }
        }
        if(allocateCounter.incrementAndGet()%100==0) {
            logger.info(String.format("allocate %d %s,this time allocate info", size, direct ? "direct" : "heap"));
            memoryMonitor();
        }
         bufferSizeAllocated.getAndAdd(size);
         return true;
    }

    /**
     *
     **/
    public void memoryMonitor(){
       logger.info( String.format("allocated direct buffer %d,heap %d",allocatedDirectBufferSize.get(),allocatedHeapBufferSize.get()));
    }

    @Override
    public boolean onRelease(int size,boolean direct) {
        if(direct){
            allocatedDirectBufferSize.addAndGet(-size);
        }else {
            allocatedDirectBufferSize.addAndGet(-size);
        }
        bufferSizeAllocated.getAndAdd(-size);
        return true;
    }

    @Override
    public boolean onRelease(ByteBuffer buffer) {
                release(buffer);
        return  onRelease(buffer.capacity(),buffer.isDirect());
    }

    @Override
    public void close() throws IOException {
        heapCache.clear();
        BufferHolder holder;
        do {
            holder=directCache.poll();
            if(holder!=null) {
                onRelease(holder.value().capacity(),true);
                release(holder.value());
            }
        }while (holder!=null);
    }
}
