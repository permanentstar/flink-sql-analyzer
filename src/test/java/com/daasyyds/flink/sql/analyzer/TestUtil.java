package com.daasyyds.flink.sql.analyzer;

import com.daasyyds.flink.sql.analyzer.common.Tuple2;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TestUtil {
    private static final String SQL_FILE_SPLITTER = "----------";

    public static Tuple2<String, List<String>> sqlToTest(List<String> paths) {
        List<String> sqls = loadSqls(paths);
        String allSqls = String.join(";\n", sqls);
        return Tuple2.of(allSqls, sqls);
    }

    public static String combineSqls(List<String> paths) {
        List<String> sqls = loadSqls(paths);
        return String.join(";\n", sqls);
    }

    public static List<String> loadSqls(List<String> paths) {
        try {
            List<String> sqls = new ArrayList<>(paths.size());
            for (String p : paths) {
                String ss = resourceToString(p);
                sqls.addAll(Stream.of(ss.split(SQL_FILE_SPLITTER, -1)).map(String::trim).collect(Collectors.toList()));
            }
            return sqls;
        } catch (IOException e) {
            throw new RuntimeException(e.getCause());
        }
    }

    private static String resourceToString(String name) throws IOException {
        return IOUtils.toString(resourceToURL(name), StandardCharsets.UTF_8);
    }

    private static URL resourceToURL(String name) {
        URL resource = IOUtils.class.getResource(name);
        if (resource == null) {
            throw new RuntimeException("Resource not found: " + name);
        } else {
            return resource;
        }
    }
}
