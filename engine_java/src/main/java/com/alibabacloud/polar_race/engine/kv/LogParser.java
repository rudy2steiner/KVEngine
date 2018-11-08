package com.alibabacloud.polar_race.engine.kv;

import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.common.io.FileChannelIOHandlerImpl;
import com.alibabacloud.polar_race.engine.common.io.IOHandler;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;

public class LogParser {

     private byte[] key;
     private byte[] value;
     private String dir;
     private String filename;
     private IOHandler handler;
    public LogParser(String dir,String filename) throws FileNotFoundException {
        this.dir=dir;
        this.filename=filename;
        this.handler=new FileChannelIOHandlerImpl(dir,filename,"rw",-1);
        this.key=new byte[StoreConfig.KEY_SIZE];
        this.value=new byte[StoreConfig.VALUE_SIZE];
    }

    /**
     * @param  to in write mode
     * @param
     **/
    public void parse(AbstractVisitor visitor, ByteBuffer to,ByteBuffer from) throws IOException {
         int capacity=to.capacity();
         int remain=0;
         int i=0;
         do{
              i++;
              handler.read(to);
              to.flip();
              remain=to.remaining();
              batchVisit(to,visitor);
              if(to.hasRemaining()){
                  from.put(to);
                  to.clear();
                  from.flip();
                  to.put(from);
                  from.clear();
              }
         }while (remain==capacity);
    }

    /**
     *
     * @return  position
     */
    int batchVisit(ByteBuffer buffer,AbstractVisitor visitor){
        int len=0;
        while(buffer.remaining()>=2){
            buffer.mark();
            len=buffer.getShort();
            if(len==0){
                buffer.position(buffer.limit());
                // end of file
                return buffer.limit();
            }
            if(buffer.remaining()<len){
                // 回退
                buffer.reset();
                return buffer.position();
            }
            buffer.get(key);
            if(len- key.length==value.length) {
                buffer.get(value);
                visitor.visit(key, value);
            }else{
                byte[] valueTmp=new byte[len-key.length];
                buffer.get(valueTmp);
                visitor.visit(key,valueTmp);
            }
        }
          return  buffer.position();
    }
}
