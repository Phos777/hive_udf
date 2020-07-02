package com.sudoprivacy.utils;

import org.apache.hadoop.io.IntWritable;

public class UdfConvert {
    public static Integer toInt(Object o) {
        if (o instanceof IntWritable) {
            return ((IntWritable) o).get();
        } else {
            return (Integer) o;
        }
    }
}
