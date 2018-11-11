package com.alibabacloud.polar_race.engine.common.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;

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

}
