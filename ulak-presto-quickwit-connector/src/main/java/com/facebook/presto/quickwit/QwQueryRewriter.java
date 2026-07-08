package com.facebook.presto.quickwit;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

public class QwQueryRewriter {

    @SuppressWarnings("unchecked")
    public static String rewriteQueryForHistory(String queryJson) {
        if (StringUtils.isBlank(queryJson)) {
            return queryJson;
        }
        try {
            Gson gson = new GsonBuilder().serializeNulls().create();
            Map<String, Object> queryMap = gson.fromJson(queryJson, Map.class);
            if (queryMap != null && queryMap.containsKey("aggs")) {
                Object aggsObj = queryMap.get("aggs");
                if (aggsObj instanceof Map) {
                    rewriteAggsForHistory((Map<String, Object>) aggsObj);
                }
            }
            return gson.toJson(queryMap);
        } catch (Exception e) {
            return queryJson;
        }
    }

    @SuppressWarnings("unchecked")
    private static void rewriteAggsForHistory(Map<String, Object> aggsMap) {
        if (aggsMap == null) return;
        for (Map.Entry<String, Object> entry : aggsMap.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (value instanceof Map) {
                Map<String, Object> valMap = (Map<String, Object>) value;
                if ("min".equals(key) || "max".equals(key) || "avg".equals(key) || "sum".equals(key) || "value_count".equals(key)) {
                    Object fieldObj = valMap.get("field");
                    if (fieldObj instanceof String) {
                        String field = (String) fieldObj;
                        if (field.startsWith("span_attributes.")) {
                            String fieldName = field.substring("span_attributes.".length());
                            if (!fieldName.endsWith("_min") && !fieldName.endsWith("_max") && !fieldName.endsWith("_avg") && !fieldName.endsWith("_sum") && !fieldName.endsWith("_count")) {
                                String suffix = "value_count".equals(key) ? "count" : key;
                                valMap.put("field", "span_attributes." + fieldName + "_" + suffix);
                            }
                        }
                    }
                } else {
                    rewriteAggsForHistory(valMap);
                }
            } else if (value instanceof List) {
                for (Object item : (List<Object>) value) {
                    if (item instanceof Map) {
                        rewriteAggsForHistory((Map<String, Object>) item);
                    }
                }
            }
        }
    }
}
