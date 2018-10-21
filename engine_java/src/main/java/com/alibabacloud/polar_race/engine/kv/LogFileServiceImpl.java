package com.alibabacloud.polar_race.engine.kv;

import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.common.io.FileChannelIOHandlerImpl;
import com.alibabacloud.polar_race.engine.common.io.IOHandler;
import com.alibabacloud.polar_race.engine.common.utils.Bytes;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class LogFileServiceImpl implements LogFileService{
    private String dir;

    public LogFileServiceImpl(String dir){
        this.dir=dir;
    }
    @Override
    public String nextLogName(Cell cell) {
        return Bytes.bytes2long(cell.getKey(),0)+ StoreConfig.LOG_FILE_SUFFIX;
    }

    @Override
    public IOHandler getFileChannelIOHandler(String fileName, int bufferSize) throws FileNotFoundException {
        return new FileChannelIOHandlerImpl(dir,fileName,"rw",bufferSize);
    }

    @Override
    public List<String> allLogFiles() {
        File file=new File(dir);
        List<String> logNames=new ArrayList<>();
        if(!file.isDirectory()) return null;

        String[] names= file.list() ;
        for(String name:names){
            if(name.endsWith(StoreConfig.LOG_FILE_SUFFIX)){
                logNames.add(name);
            }
        }
        Collections.sort(logNames);
        return logNames;
    }
}
