package com.facebook.presto.quickwit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.*;

public final class AggsDslCompiler {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private AggsDslCompiler() {}

    public static String normalizeAggs(String aggsArg) {
        if (aggsArg == null) return null;
        String s = aggsArg.trim();
        if (s.isEmpty()) return s;

        // JSON passthrough
        if (s.startsWith("{") || s.startsWith("[{")) return s;

        // DSL
        if (s.startsWith("[")) {
            return compileDslToJson(s);
        }

        throw new IllegalArgumentException("aggs must be JSON or DSL list");
    }

    private static String compileDslToJson(String dsl) {
        String inner = stripOuterBrackets(dsl.trim());
        List<String> items = splitTopLevel(inner, ',');

        Histogram hist = null;
        List<Terms> termsList = new ArrayList<Terms>();
        // all metrics declared by DSL (by id)
        LinkedHashMap<String, Metric> metrics = new LinkedHashMap<String, Metric>();

        for (int i = 0; i < items.size(); i++) {
            String item = items.get(i).trim();
            if (item.isEmpty()) continue;

            FnCall call = parseFnCall(item);
            Map<String, String> args = parseArgs(call.args);
            String name = call.name;

            if ("histogram".equals(name)) {
                if (hist != null) throw new IllegalArgumentException("Only one histogram(...) is allowed");
                hist = parseHistogram(args);
            } else if ("terms".equals(name)) {
                termsList.add(parseTerms(args));
            } else if (isMetricFn(name)) {
                Metric m = parseMetric(name, args);
                if (metrics.containsKey(m.id)) {
                    throw new IllegalArgumentException("Duplicate metric id: " + m.id);
                }
                metrics.put(m.id, m);
            } else {
                throw new IllegalArgumentException("Unsupported function '" + name + "'");
            }
        }

    // NEW: histogram/terms required değil; ama hiç agg yoksa hata
    if (hist == null && termsList.isEmpty() && metrics.isEmpty()) {
        throw new IllegalArgumentException("DSL must contain at least one aggregation (histogram/terms/metric).");
    }

        // 1) Determine which metrics must live at which terms level due to ordering
        // orderNeeds[levelIndex] = metricId
        Map<Integer, String> orderNeeds = new HashMap<Integer, String>();
        Map<Integer, String> orderDir = new HashMap<Integer, String>();

        for (int i = 0; i < termsList.size(); i++) {
            Terms t = termsList.get(i);
            if (t.orderMetricId != null && !t.orderMetricId.isEmpty()) {
                orderNeeds.put(i, t.orderMetricId);
                orderDir.put(i, (t.orderDir == null || t.orderDir.isEmpty()) ? "desc" : t.orderDir);
            }
        }

        // 2) Validate that referenced order metrics exist in DSL, or auto-create (optional)
        // Here: we REQUIRE they exist (safer)
        for (Map.Entry<Integer, String> e : orderNeeds.entrySet()) {
            String mid = e.getValue();
            if (!metrics.containsKey(mid)) {
            throw new IllegalArgumentException(
                "terms(order=id:" + mid + ") but metric id '" + mid + "' is not defined in DSL"
            );
            }
        }

    ObjectNode root = MAPPER.createObjectNode();

    // NEW: topAggsContainer = nereye aggs yazacağız?
    ObjectNode topAggsContainer;

    if (hist != null) {
        String histId = (hist.id != null && !hist.id.isEmpty()) ? hist.id : "3";

        ObjectNode histNode = MAPPER.createObjectNode();

        ObjectNode dh = MAPPER.createObjectNode();
        dh.put("field", hist.field);
        dh.put("fixed_interval", hist.interval);
        dh.put("min_doc_count", hist.minDocCount);
        histNode.set("date_histogram", dh);

        ObjectNode histAggs = MAPPER.createObjectNode();
        histNode.set("aggs", histAggs);

        root.set(histId, histNode);
        topAggsContainer = histAggs;
    } else {
        // histogram yoksa root direkt aggs map gibi kullanılır
        topAggsContainer = root;
    }

    // terms zincirini kur (varsa)
        List<ObjectNode> termAggsContainers = new ArrayList<ObjectNode>();

    ObjectNode currentContainer = topAggsContainer;

        for (int i = 0; i < termsList.size(); i++) {
            Terms t = termsList.get(i);
            String termsId = (t.id != null && !t.id.isEmpty()) ? t.id : defaultTermsId(i);

            ObjectNode termsNode = MAPPER.createObjectNode();
            ObjectNode termsObj = MAPPER.createObjectNode();

            termsObj.put("field", t.field);
            termsObj.put("size", t.size);
            termsObj.put("min_doc_count", t.minDocCount);

            // order (if present, set it now; metric injection happens later)
            String mid = orderNeeds.get(i);
            if (mid != null) {
                String dir = orderDir.get(i);
                dir = (dir == null ? "desc" : dir.trim().toLowerCase(Locale.ROOT));
                if (!"asc".equals(dir) && !"desc".equals(dir)) {
                    throw new IllegalArgumentException("Invalid order direction: " + dir);
                }
                ObjectNode ord = MAPPER.createObjectNode();
                ord.put(mid, dir);
                termsObj.set("order", ord);
            }

            termsNode.set("terms", termsObj);

            ObjectNode nextAggs = MAPPER.createObjectNode();
            termsNode.set("aggs", nextAggs);

            currentContainer.set(termsId, termsNode);

            termAggsContainers.add(nextAggs);
            currentContainer = nextAggs;
        }

    // metrikleri nereye koyacağız?
    ObjectNode metricsTarget =
        termsList.isEmpty()
            ? topAggsContainer
            : termAggsContainers.get(termAggsContainers.size() - 1);

    // order metriklerini ilgili terms seviyesine inject et
        for (Map.Entry<Integer, String> e : orderNeeds.entrySet()) {
            int level = e.getKey();
            String metricId = e.getValue();
            Metric m = metrics.get(metricId);
            if (m == null) continue;

            ObjectNode levelAggs = termAggsContainers.get(level);

            // only inject if not already present
            if (!levelAggs.has(metricId)) {
                levelAggs.set(metricId, makeMetricNode(m));
            }
        }

    // kalan metrikleri (ve eksikleri) metricsTarget'a koy

        for (Metric m : metrics.values()) {
        if (!metricsTarget.has(m.id)) {
            metricsTarget.set(m.id, makeMetricNode(m));
            }
        }


        try {
            return MAPPER.writeValueAsString(root);
        } catch (JsonProcessingException ex) {
            throw new RuntimeException("Failed to serialize JSON: " + ex.getMessage(), ex);
        }
    }

    private static boolean isMetricFn(String name) {
        return "sum".equals(name) || "avg".equals(name) || "min".equals(name) || "max".equals(name) || "count".equals(name);
    }

    private static ObjectNode makeMetricNode(Metric m) {
        ObjectNode wrapper = MAPPER.createObjectNode();
        ObjectNode spec = MAPPER.createObjectNode();
        spec.put("field", m.field);
        String qwAgg = "count".equals(m.kind) ? "value_count" : m.kind;
        wrapper.set(qwAgg, spec);
        return wrapper;
    }

    // ---- defaults for term IDs if not provided ----
    private static String defaultTermsId(int idx) {
        // first default "4", second default "5", then "6"... (change if you want)
        if (idx == 0) return "4";
        return String.valueOf(4 + idx);
    }

    // ---------------- models ----------------
    private static final class Histogram {
        final String id, field, interval;
        final int minDocCount;
        Histogram(String id, String field, String interval, int minDocCount) {
            this.id = id; this.field = field; this.interval = interval; this.minDocCount = minDocCount;
        }
    }

    private static final class Terms {
        final String id, field, orderMetricId, orderDir;
        final int size, minDocCount;
        Terms(String id, String field, int size, String orderMetricId, String orderDir, int minDocCount) {
            this.id = id; this.field = field; this.size = size;
            this.orderMetricId = orderMetricId; this.orderDir = orderDir;
            this.minDocCount = minDocCount;
        }
    }

    private static final class Metric {
        final String id, field, kind;
        Metric(String id, String field, String kind) { this.id = id; this.field = field; this.kind = kind; }
    }

    private static final class FnCall {
        final String name, args;
        FnCall(String name, String args) { this.name = name; this.args = args; }
    }

    // ---------------- parsing ----------------
    private static Histogram parseHistogram(Map<String, String> args) {
        String field = require(args, "field");
        String interval = require(args, "interval");
        String id = args.get("id");
        int min = parseIntOrDefault(args.get("min"), 1);
        return new Histogram(id, field, interval, min);
    }

    private static Terms parseTerms(Map<String, String> args) {
        String field = require(args, "field");
        int size = parseIntOrDefault(args.get("size"), 10);
        String id = args.get("id");
        int min = parseIntOrDefault(args.get("min"), 1);

        // order=id:11:desc
        String order = args.get("order");
        String orderMetricId = null;
        String orderDir = "desc";
        if (order != null && !order.trim().isEmpty()) {
            String o = order.trim();
            if (!o.startsWith("id:")) {
                throw new IllegalArgumentException("terms.order must be like order=id:11 or order=id:11:asc");
            }
            String[] parts = o.split(":");
            if (parts.length < 2) throw new IllegalArgumentException("terms.order missing metric id (order=id:11)");
            orderMetricId = parts[1].trim();
            if (orderMetricId.isEmpty()) throw new IllegalArgumentException("terms.order metric id is empty");
            if (parts.length >= 3) orderDir = parts[2].trim();
        }

        return new Terms(id, field, size, orderMetricId, orderDir, min);
    }

    private static Metric parseMetric(String kind, Map<String, String> args) {
        String id = require(args, "id");
        String field = require(args, "field");
        return new Metric(id, field, kind);
    }

    private static FnCall parseFnCall(String item) {
        int p = item.indexOf('(');
        int q = item.lastIndexOf(')');
        if (p <= 0 || q <= p) throw new IllegalArgumentException("Expected name(...) but got: " + item);
        String name = item.substring(0, p).trim();
        String args = item.substring(p + 1, q).trim();
        if (name.isEmpty()) throw new IllegalArgumentException("Empty function name: " + item);
        return new FnCall(name, args);
    }

    private static Map<String, String> parseArgs(String args) {
        LinkedHashMap<String, String> out = new LinkedHashMap<String, String>();
        if (args == null || args.trim().isEmpty()) return out;

        List<String> parts = splitTopLevel(args, ',');
        for (int i = 0; i < parts.size(); i++) {
            String kv = parts.get(i).trim();
            if (kv.isEmpty()) continue;

            int eq = kv.indexOf('=');
            if (eq <= 0) throw new IllegalArgumentException("Expected key=value but got: " + kv);

            String k = kv.substring(0, eq).trim();
            String v = stripOptionalQuotes(kv.substring(eq + 1).trim());
            if (k.isEmpty()) throw new IllegalArgumentException("Empty key in: " + kv);
            out.put(k, v);
        }
        return out;
    }

    private static String require(Map<String, String> args, String key) {
        String v = args.get(key);
        if (v == null || v.trim().isEmpty()) throw new IllegalArgumentException("Missing required arg '" + key + "'");
        return v.trim();
    }

    private static int parseIntOrDefault(String s, int def) {
        if (s == null || s.trim().isEmpty()) return def;
        try { return Integer.parseInt(s.trim()); }
        catch (NumberFormatException e) { throw new IllegalArgumentException("Invalid integer: " + s); }
    }

    private static String stripOptionalQuotes(String v) {
        String s = v.trim();
        if (s.length() >= 2) {
            if ((s.startsWith("\"") && s.endsWith("\"")) || (s.startsWith("'") && s.endsWith("'"))) {
                return s.substring(1, s.length() - 1);
            }
        }
        return s;
    }

    private static String stripOuterBrackets(String s) {
        if (!s.startsWith("[") || !s.endsWith("]")) throw new IllegalArgumentException("DSL must be [ ... ]");
        return s.substring(1, s.length() - 1).trim();
    }

    private static List<String> splitTopLevel(String s, char delimiter) {
        List<String> parts = new ArrayList<String>();
        StringBuilder cur = new StringBuilder();
        int paren = 0;
        boolean inSingle = false, inDouble = false;

        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);

            if (c == '\'' && !inDouble) inSingle = !inSingle;
            else if (c == '"' && !inSingle) inDouble = !inDouble;

            if (!inSingle && !inDouble) {
                if (c == '(') paren++;
                else if (c == ')' && paren > 0) paren--;
            }

            if (c == delimiter && paren == 0 && !inSingle && !inDouble) {
                parts.add(cur.toString());
                cur.setLength(0);
            } else {
                cur.append(c);
            }
        }
        parts.add(cur.toString());
        return parts;
    }



// ---- tiny demo ----
    public static void main(String[] args) {
        String dsl =
                "[\n" +
                        "  histogram(field=span_start_timestamp_nanos, interval=1h, id=3),\n" +
                        "  terms(field=span_attributes.n, size=10, order=id:1:desc, min=1, id=4),\n" +
                        "  terms(field=span_attributes.v, size=10, order=id:1:desc, min=1, id=5),\n" +
                        "  sum(id=1,  field=span_attributes.u),\n" +
                        "  avg(id=2,  field=span_attributes.t),\n" +
                        "  min(id=13, field=span_attributes.ab),\n" +
                        "  max(id=14, field=span_attributes.ac),\n" +
                        "  count(id=12, field=span_attributes.u)\n" +
                        "]";

        System.out.println(normalizeAggs(dsl));
        String dsl2 =
                "{\"3\":{\"date_histogram\":{\"field\":\"span_start_timestamp_nanos\",\"fixed_interval\":\"1h\",\"min_doc_count\":1},\"aggs\":{\"4\":{\"terms\":{\"field\":\"span_attributes.n\",\"size\":10,\"min_doc_count\":1,\"order\":{\"1\":\"desc\"}},\"aggs\":{\"5\":{\"terms\":{\"field\":\"span_attributes.v\",\"size\":10,\"min_doc_count\":1,\"order\":{\"1\":\"desc\"}},\"aggs\":{\"1\":{\"sum\":{\"field\":\"span_attributes.u\"}},\"2\":{\"avg\":{\"field\":\"span_attributes.t\"}},\"13\":{\"min\":{\"field\":\"span_attributes.ab\"}},\"14\":{\"max\":{\"field\":\"span_attributes.ac\"}},\"12\":{\"value_count\":{\"field\":\"span_attributes.u\"}}}},\"1\":{\"sum\":{\"field\":\"span_attributes.u\"}}}}}}}\n";

        System.out.println(normalizeAggs(dsl2));

        System.out.println(dsl.equals(dsl2) );

    }
}
