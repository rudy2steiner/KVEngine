package com.alibabacloud.polar_race.engine.kv.cache;


import com.alibabacloud.polar_race.engine.common.Service;
import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.common.io.IOHandler;
import com.alibabacloud.polar_race.engine.kv.file.LogFileService;

import java.util.List;


/**
 * cache io handler to avoid open file too frequency
 *
 **/
public class IOHandlerLRUCache extends Service {

    private IOHandler[] ioHandlers;
    private LogFileService logFileService;
    private List<Long> files;
    public IOHandlerLRUCache(LogFileService logFileService){
        this.logFileService=logFileService;
        this.files=logFileService.allLogFiles();
        this.ioHandlers=new IOHandler[files.size()];
    }

    @Override
    public void onStart() throws Exception {
         ioHandlers=new IOHandler[files.size()];
         int fileIndex;
         for(Long fid:files){
             fileIndex=((int)(fid/StoreConfig.SEGMENT_LOG_FILE_SIZE));
             ioHandlers[fileIndex]=logFileService.ioHandler(fid+ StoreConfig.LOG_FILE_SUFFIX,"r");
         }
    }

    /***
     * @return  io handler  or null
     *
     **/
    public IOHandler get(long fileId){
        int fileIndex=((int)(fileId/StoreConfig.SEGMENT_LOG_FILE_SIZE));
            if(fileIndex<ioHandlers.length){
               return ioHandlers[fileIndex];
            }
        return null;
    }

    /**
     * log 文件数量
     **/
    public int size(){
        return files.size();
    }

    public List<Long> files(){
        return files;
    }

    @Override
    public void onStop() throws Exception {
        this.ioHandlers = null;
    }
}
