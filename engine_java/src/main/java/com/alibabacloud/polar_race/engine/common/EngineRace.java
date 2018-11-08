package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.AbstractEngine;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.alibabacloud.polar_race.example.LogRingBufferEngine;

public class EngineRace extends AbstractEngine {

     private AbstractEngine engine=new LogRingBufferEngine();
	@Override
	public void open(String path) throws EngineException {
		engine.open(path);
	}
	
	@Override
	public void write(byte[] key, byte[] value) throws EngineException {
		engine.write(key,value);
	}
	
	@Override
	public byte[] read(byte[] key) throws EngineException {
		return engine.read(key);
	}
	
	@Override
	public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) throws EngineException {

	}
	
	@Override
	public void close() {
		engine.close();
	}

}
