package com.alibabacloud.polar_race.engine.common.io;

import java.util.LinkedList;
import java.util.List;

public class FileChannelManager {
    private String root;
    private List<IOHandler>  handlers;
    public FileChannelManager(String root){
        this.root=root;
        this.handlers=new LinkedList();
    }
    public void close(IOHandler handler){
           this.handlers.add(handler);
    }



}
