package com.alibabacloud.polar_race.example;

import com.alibabacloud.polar_race.engine.common.AbstractEngine;
import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.io.LogRingBuffer;

public class RingBufferEngine extends AbstractEngine {
    private LogRingBuffer logger;
    public RingBufferEngine(){
        this.logger=new LogRingBuffer(1024*8*8);
    }
    @Override
    public void open(String path) throws EngineException {
        this.logger.start();
    }

    @Override
    public void close() {
        this.logger.close();
    }

    @Override
    public void write(byte[] key, byte[] value) throws EngineException {
        try {
            logger.publish(key, value);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public byte[] read(byte[] key) throws EngineException {
        return new byte[0];
    }

    @Override
    public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) throws EngineException {

    }
}
