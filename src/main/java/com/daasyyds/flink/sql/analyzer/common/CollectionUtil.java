package com.daasyyds.flink.sql.analyzer.common;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class CollectionUtil {
    public static boolean isEmpty(Collection<?> collection) {
        return (collection == null || collection.isEmpty());
    }

    public static boolean notEmpty(Collection<?> collection) {
        return !isEmpty(collection);
    }

    public static Map<String, Object> parseDotPath2NestedMap(Map<String, Object> raw) {
        Map<String, Object> map = new LinkedHashMap<>();
        for(Map.Entry<String, Object> entry : raw.entrySet()) {
            List<String> paths = Arrays.asList(entry.getKey().split("\\.", -1));
            getOrCreateNestMap(map, paths).put(paths.get(paths.size()-1), entry.getValue());
        }
        return map;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> getOrCreateNestMap(Map<String, Object> parent, List<String> paths) {
        Object nested = parent.get(paths.get(0));
        if (nested != null && !(nested instanceof Map)) {
            throw new RuntimeException(String.format("parent node of nest path %s must be map", String.join(".", paths)));
        }
        if (nested == null) {
            nested = new LinkedHashMap<String, Object>();
            parent.put(paths.get(0), nested);
        }
        if (paths.size() == 1) {
            return parent;
        }
        return getOrCreateNestMap((Map<String, Object>)nested, paths.subList(1, paths.size()));
    }

    public static Map<String, Object> parseNestedMapToDotPathMap(Map<String, Object> raw) {
        return parseNestedMapToDotPathMap(null, raw);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> parseNestedMapToDotPathMap(String key, Map<String, Object> value) {
        Map<String, Object> finalMap = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : value.entrySet()) {
            String childKey = StringUtil.isEmpty(key) ? entry.getKey() : String.format("%s.%s", key, entry.getKey());
            if (entry.getValue() instanceof Map) {
                finalMap.putAll(parseNestedMapToDotPathMap(childKey, (Map<String, Object>) entry.getValue()));
            } else {
                finalMap.put(childKey, entry.getValue());
            }
        }
        return finalMap;
    }

    public static Object getValueInNestedMap(Map<String, Object> map, String path) {
        List<String> paths = Arrays.asList(path.split("\\."));
        return getValueInNestedMap(map, paths);
    }

    @SuppressWarnings("unchecked")
    private static Object getValueInNestedMap(Map<String, Object> map, List<String> paths) {
        if (!map.containsKey(paths.get(0))) {
            return null;
        }
        Object nestMap = map.get(paths.get((0)));
        if (paths.size() == 1) {
            return nestMap;
        }
        if (!(nestMap instanceof Map)) {
            throw new RuntimeException(String.format("parent node of nest path %s must be map", String.join(".", paths)));
        }
        return getValueInNestedMap((Map<String, Object>)nestMap, paths.subList(1, paths.size()));
    }
}
