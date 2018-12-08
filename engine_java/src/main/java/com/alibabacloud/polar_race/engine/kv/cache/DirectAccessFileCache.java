package com.alibabacloud.polar_race.engine.kv.cache;

import com.alibabacloud.polar_race.engine.common.Service;
import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.kv.file.LogFileService;
import net.smacke.jaydio.DirectRandomAccessFile;

import java.io.File;
import java.util.List;

public class DirectAccessFileCache extends Service {
    private DirectRandomAccessFile[] ioHandlers;
    private LogFileService logFileService;
    private List<Long> files;
    public DirectAccessFileCache(LogFileService logFileService){
        this.logFileService=logFileService;
        this.files=logFileService.allLogFiles();
        this.ioHandlers=new DirectRandomAccessFile[files.size()];
    }


    @Override
    public void onStart() throws Exception {
        int fileIndex;
        for(Long fid:files){
            fileIndex=((int)(fid/ StoreConfig.SEGMENT_LOG_FILE_SIZE));
            ioHandlers[fileIndex]=new DirectRandomAccessFile(new File(fileIndex+StoreConfig.LOG_FILE_SUFFIX), "r", 8*1024);
        }
    }

    /***
     * @return  io handler  or null
     *
     **/
    public DirectRandomAccessFile get(long fileId){
        int fileIndex=((int)(fileId/StoreConfig.SEGMENT_LOG_FILE_SIZE));
        if(fileIndex<ioHandlers.length){
            return ioHandlers[fileIndex];
        }
        return null;
    }
    /***
     * @return  io handler  or null
     *
     **/
    public DirectRandomAccessFile getHandler(int id){
        if(id<ioHandlers.length){
            return ioHandlers[id];
        }
        return null;
    }

    /**
     * put 文件数量
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
