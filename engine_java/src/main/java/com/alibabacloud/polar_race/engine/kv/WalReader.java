package com.alibabacloud.polar_race.engine.kv;

import java.nio.ByteBuffer;

public interface WalReader{


    /**
     * read the index of the log segment into buffer
     *
     **/
    void readWalLogIndex(String logName,ByteBuffer indexBuffer);

    /**
     *  accident close and recovery last log file index
     * @return tailer and index buffer
     *
     **/
    void  recoveryLogIndex(String logName,ByteBuffer indexBuffer);
}
