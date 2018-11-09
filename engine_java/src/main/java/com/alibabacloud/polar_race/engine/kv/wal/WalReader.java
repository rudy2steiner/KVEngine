package com.alibabacloud.polar_race.engine.kv.wal;

import com.alibabacloud.polar_race.engine.common.io.IOHandler;
import com.alibabacloud.polar_race.engine.kv.file.LogFileService;
import java.nio.ByteBuffer;




public class WalReader {
    private LogFileService logFileService;
    public WalReader(LogFileService logFileService){
        this.logFileService=logFileService;
    }

    public ByteBuffer read(String name, ByteBuffer byteBuffer,int maxLength) throws Exception {
        IOHandler handler=logFileService.ioHandler(name,"r");
                  maxLength=(int)Math.min(handler.length(),maxLength);
                  if(byteBuffer==null){
                      byteBuffer=ByteBuffer.allocate(maxLength);
                  }
                  byteBuffer.clear();
                  byteBuffer.limit(maxLength);
                  handler.read(byteBuffer);
                  logFileService.asyncCloseFileChannel(handler);
                  return byteBuffer;
    }
}
