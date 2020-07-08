package com.sudoprivacy.udf;

import com.sudoprivacy.utils.DateParser;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.joda.time.DateTimeConstants;
import org.joda.time.LocalDate;

@Description(
        name = "is_weekend",
        value = "Return 1 if the input str represents a weekend day. Else return 0.",
        extended = "Example:\n " +
                "> SELECT is_weekend(\"2020-01-01\");\n" +
                "0"
)
public class IsWeekend extends UDF {

    public IntWritable evaluate(Text input) throws HiveException {
        if (null == input) {
            throw new HiveException("Invalid input NULL for function is_weekend.");
        }
        String str_input = input.toString();
        LocalDate date = DateParser.StrToDatetime(str_input);
        if (date.getDayOfWeek() == DateTimeConstants.SATURDAY || date.getDayOfWeek() == DateTimeConstants.SUNDAY) {
            return new IntWritable(1);
        }
        return new IntWritable(0);
    }
}