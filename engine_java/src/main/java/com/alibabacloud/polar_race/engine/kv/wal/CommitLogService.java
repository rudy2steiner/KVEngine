package com.alibabacloud.polar_race.engine.kv.wal;

import com.alibabacloud.polar_race.engine.common.Service;
import com.alibabacloud.polar_race.engine.common.io.IOHandler;
import com.alibabacloud.polar_race.engine.kv.file.LogFileService;

public class CommitLogService extends Service implements KVLogger {


    public CommitLogService(IOHandler handler, LogFileService logFileService){



    }
    @Override
    public long log(byte[] key, byte[] value) throws Exception {
        return 0;
    }
}
