package com.alibabacloud.polar_race.engine.kv.buffer;

import java.nio.ByteBuffer;

public class BufferAware implements BufferSizeAware {
    private LogBufferAllocator bufferAllocator;
    public BufferAware(LogBufferAllocator allocator){
        this.bufferAllocator=allocator;
    }
    @Override
    public boolean onAdd(int size, boolean direct) {
        return bufferAllocator.onAdd(size,direct);
    }

    @Override
    public boolean onRelease(int size, boolean direct) {
        return bufferAllocator.onRelease(size,direct);
    }

    @Override
    public boolean onRelease(ByteBuffer buffer) {
        return bufferAllocator.onRelease(buffer);
    }
}
