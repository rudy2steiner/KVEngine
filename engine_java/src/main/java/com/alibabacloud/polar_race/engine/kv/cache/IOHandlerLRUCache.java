package com.alibabacloud.polar_race.engine.kv.cache;


import com.alibabacloud.polar_race.engine.common.Service;
import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.common.io.FileChannelDirectIOHandler;
import com.alibabacloud.polar_race.engine.common.io.IOHandler;
import com.alibabacloud.polar_race.engine.kv.file.LogFileService;

import java.io.File;
import java.util.BitSet;
import java.util.List;


/**
 * cache io handler to avoid open file too frequency
 *
 **/
public class IOHandlerLRUCache extends Service {

    private IOHandler[] ioHandlers;
    private LogFileService logFileService;
    private List<Long> files;
    private TransferStat[] transferStats;
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
             ioHandlers[fileIndex]=logFileService.ioHandler(fid+ StoreConfig.LOG_FILE_SUFFIX,"r");//new FileChannelDirectIOHandler(logFileService.file(fid, StoreConfig.LOG_FILE_SUFFIX),"r");//
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
    /***
     * @return  io handler  or null
     *
     **/
    public IOHandler getHandler(int id){
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
    /**
     * invoke before tansfer
     *
     **/
    public void prepareToTransfer(){
        short maxRecords=(short) (logFileService.logWritableSize()/StoreConfig.LOG_ELEMENT_SIZE);
        logFileService.maxRecordInSingleFile();
        transferStats=new TransferStat[files.size()];
        for(int i=0;i<files.size();i++){
            transferStats[i]=new TransferStat(maxRecords);
        }
    }
    /**
     * transfer
     * @return  某个文件是否转移完成
     *
     **/
    public boolean transfer(int sequenceId,int position){
        return transferStats[sequenceId].tranfer(position);
    }

    /***
     * 用于统计文件的 整理进度
     **/
    public class TransferStat{
        short transfer=0;
        BitSet bitSet;
        private short maxRecords;
        public TransferStat(short maxRecords){
                this.maxRecords=maxRecords;
                this.bitSet=new BitSet(maxRecords);
        }
        /**
         * return true,if the file tranfer finish and ready to delete
         **/
        public boolean tranfer(int position){
            transfer++;
           bitSet.set(position/StoreConfig.LOG_ELEMENT_SIZE);
           if(transfer==maxRecords){
               for(int i=0;i<maxRecords;i++){
                   if(!bitSet.get(i)){
                    return false;
                   }
               }
               return true;
           }
           return false;
        }
    }
}
