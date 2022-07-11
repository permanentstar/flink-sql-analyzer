package com.daasyyds.flink.sql.analyzer.common;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StringUtil {

    public static String trim(String str, String target) {
        if (isEmpty(str) || isEmpty(target)) {
            return str == null ? null : str.trim();
        }
        return trimTail(trimHead(str.trim(), target), target);
    }

    public static String trimHead(String str, String target) {
        if (notEmpty(target) && str.startsWith(target)) {
            return trimHead(str.substring(target.length()), target);
        }
        return str;
    }

    public static String trimTail(String str, String target) {
        if (notEmpty(target) && str.endsWith(target)) {
            return trimTail(str.substring(0, str.length() - target.length()), target);
        }
        return str;
    }

    public static boolean isEmpty(String str) {
        return str == null || str.trim().equals("");
    }

    public static boolean notEmpty(String str) {
        return !isEmpty(str);
    }

    public static List<String> splitStr(String str, String delim) {
        return Stream.of(str.split(delim, -1)).collect(Collectors.toList());
    }

    public static List<String> splitStr(String str) {
        return splitStr(str, ",");
    }
}
