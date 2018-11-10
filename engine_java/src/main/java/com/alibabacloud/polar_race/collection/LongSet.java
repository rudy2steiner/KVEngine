package com.alibabacloud.polar_race.collection;


import com.koloboke.compile.KolobokeSet;

@KolobokeSet
public abstract class LongSet implements com.koloboke.collect.set.LongSet {

    public static LongSet withExpectedSize(int expectedSize) {
        return new KolobokeLongSet(expectedSize);
    }
}
