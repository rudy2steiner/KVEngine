package com.alibabacloud.polar_race.engine.kv.buffer;

import java.nio.ByteBuffer;

public interface BufferSizeAware  {
    boolean onAdd(int size,boolean direct);
    boolean onRelease(int size,boolean direct);
    boolean onRelease(ByteBuffer buffer);
}
