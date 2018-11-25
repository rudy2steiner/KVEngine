package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.example.WALogEngine;

public class EngineRace extends AbstractEngine {

     private AbstractEngine engine=new WALogEngine();
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
		engine.range(lower,upper,visitor);
	}
	
	@Override
	public void close() {
		engine.close();
	}

}
