package com.alibabacloud.polar_race.engine.kv.buffer;

import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;

public class BufferHandler {

    public static void release(ByteBuffer buffer){
        if(buffer.isDirect()){
            ((DirectBuffer)buffer).cleaner();
        }
    }
}
