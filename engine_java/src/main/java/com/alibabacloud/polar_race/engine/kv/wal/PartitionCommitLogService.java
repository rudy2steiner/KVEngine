package com.alibabacloud.polar_race.engine.kv.wal;

import com.alibabacloud.polar_race.engine.common.Service;
import com.alibabacloud.polar_race.engine.common.io.IOHandler;
import com.alibabacloud.polar_race.engine.kv.file.LogFileService;
import com.alibabacloud.polar_race.engine.kv.partition.Range;



/***
 *  multi partition  commit log
 **/
public class PartitionCommitLogService extends Service {
    private CommitLogService commitLogService;
    private Range[] partitions;
    private int partitionId;

//    public PartitionCommitLogService(IOHandler handler, LogFileService partitionLogFileService, Range[] partitions,int partitionId){
//             commitLogService=new CommitLogService(handler,partitionLogFileService);
//             this.partitions=partitions;
//             this.partitionId=partitionId;
//    }
}
