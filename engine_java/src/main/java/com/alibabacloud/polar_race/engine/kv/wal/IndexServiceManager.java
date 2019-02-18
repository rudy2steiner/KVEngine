package com.alibabacloud.polar_race.engine.kv.wal;

import com.alibabacloud.polar_race.engine.common.Service;
import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.common.io.IOHandler;
import com.alibabacloud.polar_race.engine.kv.event.SyncEvent;
import com.alibabacloud.polar_race.engine.kv.file.LogFileService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.*;


/***
 * 管理index 服务,处理启动顺序
 *
 **/
public class IndexServiceManager extends Service{
    private final static Logger logger= LoggerFactory.getLogger(IndexServiceManager.class);
    /**
     * 表示disk 剩余空间
     *
     **/
    private long diskRemaining=StoreConfig.ORDER_LOG_TRANSFER_DISK_SIZE;
    private SyncEvent[] partitionSemaphore=new SyncEvent[StoreConfig.PARTITIONS];
    private Map<Integer, LogFileService>  partitionLogFileMap=new HashMap<>();
    private int launchIndex=0; //下一个将要launch 的partition
    private List<PartitionStoreStat> list=new ArrayList<>();
    private String walHome;
    public IndexServiceManager(String walHome){
       // diskSemaphore=new Semaphore();
       this.walHome=walHome;
    }

    @Override
    public void onStart() throws Exception {
        check();
    }
    /**
     *
     *  check partition log size and so on
     *  确保没有大于50G 以上的单partition
     * */
    public void check(){
         LogFileService logFileService;
         PartitionStoreStat storeStat;
         for(int i=0;i<StoreConfig.PARTITIONS;i++){
             logFileService=partitionLogFileMap.get(i);
             storeStat=new PartitionStoreStat();
             storeStat.setId(i);
             storeStat.setSize(Long.valueOf(logFileService.lastLogName()==null?"0":logFileService.lastLogName())); // not accurate
             if(storeStat.getSize()>StoreConfig.ORDER_LOG_TRANSFER_DISK_SIZE){
                 logger.info(String.format("consider more partition or non-average partition,%d size %d ",i,storeStat.getSize()));
             }
             list.add(storeStat);
         }
         list.sort(new Comparator<PartitionStoreStat>() {
             @Override
             public int compare(PartitionStoreStat o1, PartitionStoreStat o2) {
                 return (int)(o1.getSize()-o2.getSize());
             }
         });
    }

    /***
     *
     **/
    public void put(int partitionId,LogFileService logFileService){
      partitionLogFileMap.put(partitionId,logFileService);
    }

    /**
     *  获取足够的空间用于tansfer 否则，等待
     **/
    public synchronized void get(int partitionId) {
        try {
            if (partitionId == list.get(launchIndex).getId()) {
                if (diskRemaining > list.get(launchIndex).getSize()) {
                    diskRemaining-=list.get(launchIndex).getSize();
                    launchIndex++;// move to next
                    return; //有足够空间
                } else {
                    partitionSemaphore[partitionId] = new SyncEvent((long) partitionId);
                    partitionSemaphore[partitionId].get(StoreConfig.MAX_TIMEOUT*10);
                }
            } else {
                partitionSemaphore[partitionId] = new SyncEvent((long) partitionId);
                partitionSemaphore[partitionId].get(StoreConfig.MAX_TIMEOUT*10);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * delete the underlying file of the handler
     * batch delete and try launch transfer
     *
     **/
    public  void release(int partitionId,List<IOHandler> handlers){
        long totalSize=0;
        try {
            for (IOHandler h : handlers) {
                // delete
                if(h.delete()){
                    totalSize += h.length();
                }else{
                    logger.info(String.format("partition %d ,%s delete failure",partitionId,h.name()));
                }

            }
            if(launchIndex<list.size()) {
                synchronized (this) {
                    diskRemaining += totalSize;
                    if (diskRemaining > list.get(launchIndex).getSize()) {
                        diskRemaining -= list.get(launchIndex).getSize();
                        partitionSemaphore[list.get(launchIndex).getId()].done(Long.MAX_VALUE);
                        logger.info("release transfer " + list.get(launchIndex).getId());
                        launchIndex++;
                    }
                }
            }else{
                logger.info("all partition has started to transfer");
            }
        }catch (IOException e){
            logger.info("release error",e);
        }

    }



    /**
     * partition stat
     *
     **/
    public class PartitionStoreStat{
        private int id;
        private long size;

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public long getSize() {
            return size;
        }

        public void setSize(long size) {
            this.size = size;
        }
    }


}
