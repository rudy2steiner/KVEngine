package com.alibabacloud.polar_race.engine.kv.index;
import com.alibabacloud.polar_race.engine.common.Service;
import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.kv.buffer.DoubleBuffer;
import com.alibabacloud.polar_race.engine.kv.event.TaskBus;

import java.nio.ByteBuffer;

public class IndexHashAppender  extends Service {
    private int capacity;
    private SSBucket buckets[];
    private int buckBufferSize ;
    private WalIndexLogger indexLogger;
    private String indexDir;
    private TaskBus ioCloseProcessor;
    public IndexHashAppender(String indexDir, int capacity, int buckBufferSize, TaskBus ioCloseProcessior){
        this.indexDir=indexDir;
        this.capacity=capacity;
        this.buckBufferSize=buckBufferSize;
        this.buckets=new SSBucket[capacity];
        this.ioCloseProcessor =ioCloseProcessior;
    }

    public void onStart() throws Exception{
        this.indexLogger = new WalIndexLogger(indexDir, capacity, ioCloseProcessor);
        for (int i = 0; i < capacity; i++) {
            buckets[i] = new SSBucket(i, new DoubleBuffer(buckBufferSize, true), indexLogger);
        }
        this.indexLogger.start();
    }


    @Override
    public void onStop() throws Exception {
        this.indexLogger.stop();
        for (SSBucket bucket : buckets) {
            bucket.close();
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
              // 注意死循环
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

    public static int hash(long key){
      return   (int)(key ^ (key >>> 32));
    }


}
