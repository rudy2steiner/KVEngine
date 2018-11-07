package com.alibabacloud.polar_race.engine.common.utils;

import com.alibabacloud.polar_race.engine.common.StoreConfig;

import java.io.File;

public class Files {

    public static void  makeDirIfNotExist(String dir){
        File file=new File(dir);
        if(file.exists()) return;
        file.mkdirs();
    }
    public static void  removeDirIfExist(String dir){
        File file=new File(dir);
        if(file.exists()&&file.isDirectory()){
            File[] files=file.listFiles();
            for(File f:files){
                 if(f.isDirectory()) removeDirIfExist(f.getAbsolutePath());
                 else f.delete();
            }
        } else file.delete();
    }

    public static boolean isEmptyDir(String dir) {
        File file=new File(dir);
        if(file.exists()){
            String[] files= file.list();
            if(files.length>0) return false;
        }
        return true;
    }

    public static int tableSizeFor(int cap){
        int n = cap - 1;
        n |= n >>> 1;
        n |= n >>> 2;
        n |= n >>> 4;
        n |= n >>> 8;
        n |= n >>> 16;
        return (n < 0) ? 1 : (n >= StoreConfig.MAXIMUM_BUFFER_CAPACITY) ? StoreConfig.MAXIMUM_BUFFER_CAPACITY : n + 1;
    }

}
