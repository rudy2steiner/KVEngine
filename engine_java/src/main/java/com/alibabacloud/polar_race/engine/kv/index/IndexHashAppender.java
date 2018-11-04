package com.alibabacloud.polar_race.engine.kv.index;
import com.alibabacloud.polar_race.engine.common.Lifecycle;
import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.kv.DoubleBuffer;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

public class IndexHashAppender  implements Lifecycle {
    private int capacity;
    private AtomicBoolean started=new AtomicBoolean(false);
    private SSBucket buckets[];
    private int buckBufferSize ;
    private int bufferQueueSize;
    private WalIndexLogger indexLogger;
    private String indexDir;
    public IndexHashAppender(String indexDir,int capacity,int buckBufferSize,int queueSize){
        this.indexDir=indexDir;
        this.capacity=capacity;
        this.buckBufferSize=buckBufferSize;
        this.bufferQueueSize=queueSize;
        this.buckets=new SSBucket[capacity];
    }

    public void start() throws Exception{
        if(started.get()==false) {
            this.indexLogger = new WalIndexLogger(indexDir, capacity, bufferQueueSize);
            for (int i = 0; i < capacity; i++) {
                buckets[i] = new SSBucket(i, new DoubleBuffer(buckBufferSize, true), indexLogger);
            }
            this.indexLogger.start();
            started.compareAndSet(false,true);
        }
    }

    @Override
    public boolean isStart() {
        return started.get();
    }

    @Override
    public void close() throws Exception {
        if(isStart()) {
            this.indexLogger.stop();
            for (SSBucket bucket : buckets) {
                bucket.close();
            }
            started.compareAndSet(true,false);
        }
    }



    /**
     *
     **/
    public void append(ByteBuffer index) throws Exception{
        ByteBuffer buffer;
        int bukId;
        int offset=-1;
        while(index.remaining()>=StoreConfig.VALUE_INDEX_RECORD_SIZE){
              index.mark();
              bukId=hash(index.getLong())%capacity;
              while(offset<0) offset=buckets[bukId].getNextOffset();
              index.reset();
              buffer=index.slice();
              //buffer.position(index.position());
              buffer.position(StoreConfig.VALUE_INDEX_RECORD_SIZE);
              buffer.flip();
              index.position(index.position()+StoreConfig.VALUE_INDEX_RECORD_SIZE);
              buckets[bukId].put(buffer,offset);
              offset=-1;
        }
    }

    public int hash(long key){
      return   (int)(key ^ (key >>> 32));
    }


}
