package com.alibabacloud.polar_race.engine.common.utils;

import java.util.Collection;

public class Null {

    public static boolean isEmpty(Collection collection){
        if(collection==null||collection.size()==0)return true;
        return false;
    }

    public static boolean isEmpty(String value){
        if(value==null||value.length()==0)return true;
        return false;
    }
    public static boolean isEmpty(Object value){
        if(value==null)return true;
        return false;
    }
}
