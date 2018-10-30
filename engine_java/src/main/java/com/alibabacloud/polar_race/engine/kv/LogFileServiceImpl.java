package com.alibabacloud.polar_race.engine.kv;

import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.common.io.BufferedIOHandler;
import com.alibabacloud.polar_race.engine.common.io.FileChannelIOHandler;
import com.alibabacloud.polar_race.engine.common.io.IOHandler;
import com.alibabacloud.polar_race.engine.common.utils.Bytes;
import com.alibabacloud.polar_race.engine.common.utils.Null;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class LogFileServiceImpl implements LogFileService{
    private String dir;
    private List<Long> logFiles;
    private int  logWritableSize;
    private int  logTailerAndIndexSize;
    public LogFileServiceImpl(String dir){
        this.dir=dir;
        scanFiles();
    }

    public void scanFiles(){
        this.logFiles=allLogFiles();
    }
    @Override
    public String nextLogName(Cell cell) {
        return Bytes.bytes2long(cell.getKey(),0)+ StoreConfig.LOG_FILE_SUFFIX;
    }

    @Override
    public String nextLogName(IOHandler handler) {
        return String.valueOf(Long.valueOf(handler.name())+StoreConfig.SEGMENT_LOG_FILE_SIZE)+StoreConfig.LOG_FILE_SUFFIX;
    }

    @Override
    public String nextLogName() {
        String lastLogName=lastLogName();
        return String.valueOf(Long.valueOf(lastLogName==null?String.valueOf(-StoreConfig.SEGMENT_LOG_FILE_SIZE):lastLogName)+StoreConfig.SEGMENT_LOG_FILE_SIZE)+StoreConfig.LOG_FILE_SUFFIX;
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
        //return new FileChalongnnelIOHandlerImpl(dir,fileName,"rw",bufferSize);
    }

    @Override
    public IOHandler ioHandler(String fileName) throws FileNotFoundException {
        return new FileChannelIOHandler(new File(dir,fileName),"rw");
    }

    @Override
    public File nextLogFile(Cell cell) {
        return new File(dir,nextLogName(cell));
    }

    @Override
    public List<Long> allLogFiles() {
        File file=new File(dir);
        List<Long> logNames=new ArrayList<>();
        if(!file.isDirectory()) return null;

        String[] names= file.list() ;
        for(String name:names){
            if(name.endsWith(StoreConfig.LOG_FILE_SUFFIX)){
                logNames.add(Long.valueOf(name.substring(0,name.indexOf('.'))));
            }
        }
        Collections.sort(logNames);
        return logNames;
    }

    @Override
    public String lastLogName() {
        if(!Null.isEmpty(logFiles))
           return String.valueOf(logFiles.get(logFiles.size()-1));
        return null;
    }

    @Override
    public int tailerAndIndexSize() {
        if(logTailerAndIndexSize<=0) {
            int size = (int)(StoreConfig.SEGMENT_LOG_FILE_SIZE / StoreConfig.VALUE_SIZE) * StoreConfig.KEY_SIZE;
            if (size % StoreConfig.K4_SIZE == 0) {
                logTailerAndIndexSize = size;
            } else {
                logTailerAndIndexSize=size-size % StoreConfig.K4_SIZE+StoreConfig.K4_SIZE;
            }
        }
        return logTailerAndIndexSize;

    }

    @Override
    public int logWritableSize() {
        if(logWritableSize<=0) {
           logWritableSize=StoreConfig.SEGMENT_LOG_FILE_SIZE- tailerAndIndexSize();
        }
        return logWritableSize;
    }

    @Override
    public boolean needReplayLog() {
        String lastName=lastLogName();
        if(lastName!=null) {
            File file = new File(dir,lastName );
            if (file.exists()){
                if(file.length()<StoreConfig.SEGMENT_LOG_FILE_SIZE){
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * binary search
     *
     **/
    @Override
    public String fileName(long position) {
        int mid;
        long value=position;
        long midValue=-1;
        if(Null.isEmpty(logFiles)) scanFiles();
        if (!Null.isEmpty(logFiles)) {
            int low = 0;
            int high = logFiles.size() - 1;
            while (low < high) {
                mid = low + (high - low) / 2;
                midValue = Long.valueOf(logFiles.get(mid));
                if (midValue < value) {
                    if (Long.valueOf(logFiles.get(mid + 1)) > value)
                        break;
                    low = mid + 1;
                } else if (midValue > value) {
                    high = mid - 1;
                } else break;

            }
            if (low == high) midValue = Long.valueOf(logFiles.get(high));
            return String.valueOf(midValue)+StoreConfig.LOG_FILE_SUFFIX;
        }
        return null;
    }
}
