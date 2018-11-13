package com.alibabacloud.polar_race.engine.common.utils;

import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.sun.management.OperatingSystemMXBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;


public class Memory {
    private final static Logger logger= LoggerFactory.getLogger(Memory.class);
    private final static int MB = 1024 * 1024;
    public static void limit(long size){
            String name = ManagementFactory.getRuntimeMXBean().getName();
            String pid = name.split("@")[0];
            logger.info("Pid is:" + pid+" limit "+size);
            boolean result;
            try {
                String momorySize="echo " + size + " >/sys/fs/cgroup/memory/test/memory.limit_in_bytes";
                String[] commmands= {"/bin/sh","-c",momorySize};
                Process pro=Runtime.getRuntime().exec(commmands);
                result= pro.waitFor((long)StoreConfig.MAX_TIMEOUT, TimeUnit.MILLISECONDS);

                String    setPid="echo " + pid + " >/sys/fs/cgroup/memory/test/cgroup.procs";
                          commmands[2]=setPid;
                          pro=Runtime.getRuntime().exec(commmands);
                result= pro.waitFor((long)StoreConfig.MAX_TIMEOUT, TimeUnit.MILLISECONDS);
                logger.info(setPid +" excc "+result);
            }catch (Exception e){
                logger.info("limit memory failed",e);
            }
    }

    /**
     *  read memory info from jvm
     **/
    public static MemoryInfo vmMemory(){
        MemoryInfo memoryInfo=new MemoryInfo();
        OperatingSystemMXBean osmxb =  (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        String os = System.getProperty("os.name");
        logger.info("os name:"+os);
        long physicalFree = osmxb.getFreePhysicalMemorySize() / MB;
        long physicalTotal = osmxb.getTotalPhysicalMemorySize() / MB;
        long physicalUse = physicalTotal - physicalFree;
        memoryInfo.setFree(physicalFree);
        memoryInfo.setTotal(physicalTotal);
        memoryInfo.setUsed(physicalUse);
        return memoryInfo;
    }


    public static void jvmHeap(){
        int heapTotal = (int)(Runtime.getRuntime().totalMemory()/MB);//Java 虚拟机中的内存总量,以字节为单位
        int heapAvailable = (int)(Runtime.getRuntime().freeMemory()/MB);//Java 虚拟机中的空闲内存量
        int heapMax=(int)(Runtime.getRuntime().maxMemory()/MB);
        MemoryMXBean memorymbean = ManagementFactory.getMemoryMXBean();
        MemoryUsage usage = memorymbean.getHeapMemoryUsage();
        System.out.println("INIT HEAP: " + usage.getInit());
        System.out.println("MAX HEAP: " + usage.getMax());
        System.out.println("USE HEAP: " + usage.getUsed());
        System.out.println("\nFull Information:");
        System.out.println("Heap Memory Usage: "
                + memorymbean.getHeapMemoryUsage());
        System.out.println("Non-Heap Memory Usage: "
                + memorymbean.getNonHeapMemoryUsage());
    }

    /**
     *  flush os page cache
     *  /bin/sh -c echo 1 > /proc/sys/vm/drop_caches
     *
     **/
    public static void sync(){
        try {
            String sync="sync";
            String[] commmands= {"/bin/sh","-c",sync};
            Process pro=Runtime.getRuntime().exec(commmands);
            boolean result= pro.waitFor((long)StoreConfig.MAX_TIMEOUT, TimeUnit.MILLISECONDS);
            logger.info("sync page cache exit "+result);
        }catch (Exception e){
            logger.info("sync os page cache failed",e);
        }
    }

    /**
     *  执行任意命令
     **/
    public static String execute(String cmd){
        StringBuilder resultBuilder = new StringBuilder("\n");
        try {
            Process pro=Runtime.getRuntime().exec(cmd);
            boolean result= pro.waitFor((long)StoreConfig.MAX_TIMEOUT, TimeUnit.MILLISECONDS);
            if(result) {
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(pro.getInputStream()));
                String line;
                do {
                    line = bufferedReader.readLine();
                    if (line != null) {
                        resultBuilder.append(line).append("\n");
                    }
                } while (line != null);
                logger.info("execute command and exit " + result);
            }
        }catch (Exception e){
            logger.info("execute command failed",e);
        }
        resultBuilder.setLength(resultBuilder.length()-1);
        return resultBuilder.toString();
    }



    /**
     * 清除已同步的缓存，释放缓存空间
     **/
    @Deprecated
    public static void clearPageCache(){
        try {
            String flushOSPagecache="echo 1 > /proc/sys/vm/drop_caches";
            Process pro=Runtime.getRuntime().exec(flushOSPagecache);
            boolean result= pro.waitFor((long)StoreConfig.MAX_TIMEOUT, TimeUnit.MILLISECONDS);
            logger.info("clear os page cache exit "+result);
        }catch (Exception e){
            logger.info("clear os page cache failed",e);
        }
    }


    /**
     * execute shell free -m
     * or vm_stat on MAC
     *
     **/
    public static MemoryInfo memory(){
        MemoryInfo memoryBean=new MemoryInfo();
        try {
            Process pro = Runtime.getRuntime().exec("free -m");
            boolean result = pro.waitFor((long)StoreConfig.MAX_TIMEOUT, TimeUnit.MILLISECONDS);
            if(result) {
                StringBuilder memoryInfo = new StringBuilder("\n");
                List<String> listMemory = new ArrayList<>();
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(pro.getInputStream()));
                String line;
                do {
                    line = bufferedReader.readLine();
                    if (line != null) {
                        memoryInfo.append(line).append("\n");
                        listMemory.add(line);
                    }
                } while (line != null);
                logger.info(memoryInfo.toString());
                if (listMemory.size() >= 2) {
//                    memoryBean = new MemoryInfo();
                    listMemory= parseMemory(listMemory.get(1));
                    if (listMemory.size() >= 7) {
                        memoryBean.setTotal(Long.valueOf(listMemory.get(1).trim()));
                        memoryBean.setUsed(Long.valueOf(listMemory.get(2).trim()));
                        memoryBean.setFree(Long.valueOf(listMemory.get(3).trim()));
                        memoryBean.setShared(Long.valueOf(listMemory.get(4).trim()));
                        memoryBean.setBufferCache(Long.valueOf(listMemory.get(5).trim()));
                        memoryBean.setAvailable(Long.valueOf(listMemory.get(6).trim()));
                    }
                }
            }
        }catch (Exception e){
            logger.info("exception",e);
        }
        return memoryBean;
    }
    /**
     * parse
     * Mem:         257650        8525      246362           1        2763      248690
     * to array
     * @return content array
     **/
    private static List<String> parseMemory(String memory){
        String[] arr=memory.split(" ");
        List<String> infos=new ArrayList<>();
        for(String s:arr){
            if(!Null.isEmpty(s)){
                infos.add(s.trim());
            }
        }
       return infos;
    }



}
