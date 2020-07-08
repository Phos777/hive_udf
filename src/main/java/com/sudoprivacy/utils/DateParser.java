package com.sudoprivacy.utils;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Arrays;
import java.util.List;

public class DateParser {
    public static LocalDate StrToDatetime(String input) throws HiveException {
        final List<DateTimeFormatter> formatterList = Arrays.asList(
                DateTimeFormat.forPattern("yyyy-MM-dd"),
                DateTimeFormat.forPattern("yyyyMMdd"),
                DateTimeFormat.forPattern("dd/MM/yyyy"),
                DateTimeFormat.forPattern("yyyy/MM/dd"),
                DateTimeFormat.forPattern("yyyy-MM-DD"),
                DateTimeFormat.forPattern("yyyyMMDD"),
                DateTimeFormat.forPattern("DD/MM/yyyy"),
                DateTimeFormat.forPattern("yyyy/MM/dd")

        );
        for (DateTimeFormatter formatter : formatterList) {
            try {
                return formatter.parseLocalDate(input);
            } catch (IllegalArgumentException e) {
                // pass
            }
        }
        throw new HiveException("Unsupported date format: " + input);
    }
}
