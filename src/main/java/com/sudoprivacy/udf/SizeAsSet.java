package com.sudoprivacy.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.util.Arrays;
import java.util.HashSet;

@Description(
        name = "size_as_set",
        value = "Parse str as list and get its unique elements count.",
        extended = "Example:\n " +
                "> SELECT size_as_set(\"a,a,b\");\n" +
                "2"
)
public class SizeAsSet extends UDF {

    public IntWritable evaluate(Text input) {
        Integer res = 0;
        if (null != input) {
            String str_input = input.toString();
            if (!StringUtils.isEmpty(str_input)) {
                res = new HashSet<String>(Arrays.asList(str_input.split(","))).size();
            }
        }
        return new IntWritable(res);
    }
}