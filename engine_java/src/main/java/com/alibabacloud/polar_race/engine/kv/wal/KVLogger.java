package com.alibabacloud.polar_race.engine.kv.wal;

public interface KVLogger  {
    long log(byte[] key,byte[] value) throws Exception;
}
