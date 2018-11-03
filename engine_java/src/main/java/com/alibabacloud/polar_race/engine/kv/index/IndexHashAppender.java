package com.alibabacloud.polar_race.engine.kv.index;
import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.kv.DoubleBuffer;
import java.nio.ByteBuffer;

public class IndexHashAppender {
    private int capacity;
    private SSBucket buckets[];
    private int buckBufferSize ;
    public IndexHashAppender(int capacity,int buckBufferSize){
        this.capacity=capacity;
        this.buckBufferSize=buckBufferSize;
    }

    public void start(){
        for(int i=0;i<capacity;i++){
            buckets[i]=new SSBucket(i,new DoubleBuffer(buckBufferSize,true));
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
              buffer.limit(index.position()+StoreConfig.VALUE_INDEX_RECORD_SIZE);
              buckets[bukId].put(buffer,offset);
              offset=-1;
        }
    }

    public int hash(long key){
      return   (int)(key ^ (key >>> 32));
    }


}
