package com.alibabacloud.polar_race.engine.kv.buffer;

public interface BufferSizeAware  {
    void onAdd(int size);
    void onRelease(int size);
}
