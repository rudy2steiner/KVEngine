package com.alibabacloud.polar_race.engine.common.utils;

import com.sun.management.OperatingSystemMXBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.Transient;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;


public class Memory {
    private final static Logger logger= LoggerFactory.getLogger(Memory.class);
    public static void limit(long size){
            String name = ManagementFactory.getRuntimeMXBean().getName();
            String pid = name.split("@")[0];
            logger.info("Pid is:" + pid+" limit "+size);
            int result;
            try {
                String momorySize="echo " + size + " >/sys/fs/cgroup/memory/test/memory.limit_in_bytes";
                String[] commmands= {"/bin/sh","-c",momorySize};
                Process pro=Runtime.getRuntime().exec(commmands);
                result= pro.waitFor();

                String    setPid="echo " + pid + " >/sys/fs/cgroup/memory/test/cgroup.procs";
                          commmands[2]=setPid;
                          pro=Runtime.getRuntime().exec(commmands);
                result= pro.waitFor();
                logger.info(setPid +" excc "+result);
            }catch (Exception e){
                logger.info("limit memory failed",e);
            }
    }

    /**
     *
     **/
    public static void momoryInfo(){
        int MB = 1024 * 1024;
        OperatingSystemMXBean osmxb =  (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        String os = System.getProperty("os.name");
        long physicalFree = osmxb.getFreePhysicalMemorySize() / MB;
        long physicalTotal = osmxb.getTotalPhysicalMemorySize() / MB;
        long physicalUse = physicalTotal - physicalFree;
        logger.info("操作系统的版本：" + os);
        logger.info("操作系统物理内存已用的空间为：" + physicalFree + " MB");
        logger.info("操作系统物理内存的空闲空间为：" + physicalUse + " MB");
        logger.info("操作系统总物理内存：" + physicalTotal + " MB");
    }

    /**
     *  flush os page cache
     *
     **/
    public static void sync(){
        int result;
        try {
            String flushOSPagecache="echo 1 > /proc/sys/vm/drop_caches";
            String[] commmands= {"/bin/sh","-c",flushOSPagecache};
            Process pro=Runtime.getRuntime().exec(commmands);
            result= pro.waitFor();
        }catch (Exception e){
            logger.info("flush os page cache failed",e);
        }
    }


    public static MemoryInfo memory(){
        MemoryInfo memoryBean=null;
        try {
            Process pro = Runtime.getRuntime().exec("free -m");
            int result = pro.waitFor();
            StringBuilder memoryInfo=new StringBuilder("\n");
            List<String>  listMemory=new ArrayList<>();
            BufferedReader bufferedReader=new BufferedReader(new InputStreamReader(pro.getInputStream()));
            String line;
            do {
                line= bufferedReader.readLine();
                if(line!=null) {
                    memoryInfo.append(line).append("\n");
                    listMemory.add(line);
                }
            }while (line!=null);
            if(listMemory.size()>=2){
                memoryBean=new MemoryInfo();
                String[] arr=listMemory.get(1).split("          ");
                if(arr.length==2) {
                    arr=arr[1].split("       ");
                    memoryBean.setTotal(Long.valueOf(arr[0].trim()));
                    memoryBean.setUsed(Long.valueOf(arr[1].trim()));
                    memoryBean.setFree(Long.valueOf(arr[2].trim()));
                    memoryBean.setShared(Long.valueOf(arr[3].trim()));
                    memoryBean.setBufferCache(Long.valueOf(arr[4].trim()));
                    memoryBean.setAvailable(Long.valueOf(arr[5].trim()));
                }
            }
            //logger.info(memoryInfo.toString());
        }catch (Exception e){
            logger.info("exception",e);
        }
        return memoryBean;
    }



}
