package com.alibabacloud.polar_race.engine.kv;

import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.common.io.BufferedIOHandler;
import com.alibabacloud.polar_race.engine.common.io.FileChannelIOHandler;
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
    public IOHandler bufferedIOHandler(String fileName, int bufferSize) throws FileNotFoundException {
        File file=new File(dir,fileName);
        IOHandler handler=new FileChannelIOHandler(file,"rw");
        return new BufferedIOHandler(handler,bufferSize);
        //return new FileChannelIOHandlerImpl(dir,fileName,"rw",bufferSize);
    }

    @Override
    public IOHandler bufferedIOHandler(String fileName,IOHandler handler) throws FileNotFoundException {
        File file=new File(dir,fileName);
        IOHandler newHandler=new FileChannelIOHandler(file,"rw");
        return new BufferedIOHandler(newHandler,handler.buffer());
        //return new FileChannelIOHandlerImpl(dir,fileName,"rw",bufferSize);
    }

    @Override
    public IOHandler ioHandler(String fileName) throws FileNotFoundException {
        return new FileChannelIOHandler(new File(fileName),"rw");
    }

    @Override
    public File nextLogFile(Cell cell) {
        return new File(dir,nextLogName(cell));
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
