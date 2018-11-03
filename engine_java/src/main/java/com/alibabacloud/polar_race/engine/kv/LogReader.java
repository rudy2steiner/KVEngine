package com.alibabacloud.polar_race.engine.kv;

import com.alibabacloud.polar_race.engine.common.io.LogRingBuffer;

public class LogReader {
    private String dir;
    public LogReader(String dir,LogFileService fileService){
        this.dir=dir;
    }



    public byte[] get(ValueIndex index){

        return new byte[0];
    }
    public void indexRecovery(String logName){

        
    }
}
