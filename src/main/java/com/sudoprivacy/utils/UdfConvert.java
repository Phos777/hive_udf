package com.sudoprivacy.utils;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.nio.charset.CharacterCodingException;

public class UdfConvert {
    public static Integer toInt(Object o) {
        if (o instanceof IntWritable) {
            return ((IntWritable) o).get();
        } else {
            return (Integer) o;
        }
    }

    public static String toStr(Object o) throws HiveException {
        if (o instanceof Text) {
            try {
                return Text.decode(((Text) o).getBytes());
            } catch (CharacterCodingException e) {
                throw new HiveException("Failed to convert text.");
            }
        } else {
            return o.toString();
        }
    }
}
