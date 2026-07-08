package com.facebook.presto.quickwit;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

public class QwQueryRewriter {

    public static String rewriteQueryForHistory(String queryJson) {
        if (StringUtils.isBlank(queryJson)) {
            return queryJson;
        }
        try {
            com.google.gson.JsonElement jsonElement = com.google.gson.JsonParser.parseString(queryJson);
            if (jsonElement.isJsonObject()) {
                com.google.gson.JsonObject root = jsonElement.getAsJsonObject();
                if (root.has("aggs")) {
                    rewriteAggsForHistory(root.get("aggs"));
                }
            }
            return new GsonBuilder().serializeNulls().create().toJson(jsonElement);
        } catch (Exception e) {
            return queryJson;
        }
    }

    private static void rewriteAggsForHistory(com.google.gson.JsonElement element) {
        if (element == null || element.isJsonNull()) return;
        if (element.isJsonObject()) {
            com.google.gson.JsonObject obj = element.getAsJsonObject();
            for (Map.Entry<String, com.google.gson.JsonElement> entry : obj.entrySet()) {
                String key = entry.getKey();
                com.google.gson.JsonElement val = entry.getValue();
                if (val.isJsonObject()) {
                    com.google.gson.JsonObject valObj = val.getAsJsonObject();
                    if ("min".equals(key) || "max".equals(key) || "avg".equals(key) || "sum".equals(key) || "value_count".equals(key)) {
                        if (valObj.has("field")) {
                            com.google.gson.JsonElement fieldEl = valObj.get("field");
                            if (fieldEl.isJsonPrimitive() && fieldEl.getAsJsonPrimitive().isString()) {
                                String field = fieldEl.getAsString();
                                if (field.startsWith("span_attributes.")) {
                                    String fieldName = field.substring("span_attributes.".length());
                                    if (!fieldName.endsWith("_min") && !fieldName.endsWith("_max") && !fieldName.endsWith("_avg") && !fieldName.endsWith("_sum") && !fieldName.endsWith("_count")) {
                                        String suffix = "value_count".equals(key) ? "count" : key;
                                        valObj.addProperty("field", "span_attributes." + fieldName + "_" + suffix);
                                    }
                                }
                            }
                        }
                    } else {
                        rewriteAggsForHistory(valObj);
                    }
                } else if (val.isJsonArray()) {
                    rewriteAggsForHistory(val.getAsJsonArray());
                }
            }
        } else if (element.isJsonArray()) {
            com.google.gson.JsonArray arr = element.getAsJsonArray();
            for (com.google.gson.JsonElement item : arr) {
                rewriteAggsForHistory(item);
            }
        }
    }
}
