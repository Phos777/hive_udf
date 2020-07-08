package com.sudoprivacy.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

@Description(
        name = "size_as_list",
        value = "Parse str as list and get its size.",
        extended = "Example:\n > SELECT size_as_list(col) from table;"
)
public class SizeAsList extends UDF {

    public IntWritable evaluate(Text input) {
        Integer res = 0;
        if (null != input) {
            String str_input = input.toString();
            if (!StringUtils.isEmpty(str_input)) {
                res = str_input.split(",").length;
            }
        }
        return new IntWritable(res);
    }
}
