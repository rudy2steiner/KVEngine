package com.alibabacloud.polar_race.engine.common.thread;

import java.util.concurrent.ThreadFactory;

public class NamedThreadFactory implements ThreadFactory {
    String name;
    public NamedThreadFactory(String name){
        this.name=name;
    }
    @Override
    public Thread newThread(Runnable r) {
        return new Thread(r,name);
    }
}
