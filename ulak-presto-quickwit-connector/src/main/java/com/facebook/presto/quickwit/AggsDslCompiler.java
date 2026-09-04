package com.facebook.presto.quickwit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public final class AggsDslCompiler {
    private static final Logger logger = LoggerFactory.getLogger(AggsDslCompiler.class);
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
        IdAllocator ids = new IdAllocator();
        for (int i = 0; i < items.size(); i++) {
            String item = items.get(i).trim();
            if (item.isEmpty()) continue;

            FnCall call = parseFnCall(item);
            Map<String, String> args = parseArgs(call.args);
            String name = call.name;

            if ("histogram".equals(name)) {
                if (hist != null) throw new IllegalArgumentException("Only one histogram(...) is allowed");
                hist = parseHistogram(args);
                ids.reserve(hist.id);
            } else if ("terms".equals(name)) {
                Terms t = parseTerms(args);
                ids.reserve(t.id);
                termsList.add(t);
            } else if (isMetricFn(name)) {
                Metric m = parseMetric(name, args);

                // reserve + duplicate check for explicit ids
                if (m.id != null && !m.id.trim().isEmpty()) {
                ids.reserve(m.id);
                    if (metrics.containsKey(m.id)) {
                    throw new IllegalArgumentException("Duplicate metric id: " + m.id);
                }

                // şimdilik ekle (id null olabilir, sonra rebuild edeceğiz)
                metrics.put(m.id, m);
                } else {
                    // temporary key; will rebuild after auto-id allocation
                    metrics.put("~tmp~" + i, m);
                }

            } else {
                throw new IllegalArgumentException("Unsupported function '" + name + "'");
            }
        }

        // NEW: histogram/terms required değil; ama hiç agg yoksa hata
        if (hist == null && termsList.isEmpty() && metrics.isEmpty()) {
            throw new IllegalArgumentException("DSL must contain at least one aggregation (histogram/terms/metric).");
        }

        // ---------------- Auto-ID (MUST be before JSON build) ----------------
        // histogram id auto → "date"
        if (hist != null && isBlank(hist.id)) {
            hist = new Histogram(ids.claim("date"), hist.field, hist.interval, hist.minDocCount);
        }

        // terms id auto → field name (last segment after last dot)
        for (int i = 0; i < termsList.size(); i++) {
            Terms t = termsList.get(i);
            if (isBlank(t.id)) {
                termsList.set(i, new Terms(
                        ids.claim(fieldToId(t.field)), t.field, t.size, t.orderMetricId, t.orderDir, t.minDocCount, t.missing, t.orderByKey
                ));
            }
        }

        // metrics id auto → field name + rebuild map with real ids
        LinkedHashMap<String, Metric> newMetrics = new LinkedHashMap<String, Metric>();
        for (Metric m : metrics.values()) {
            String mid = m.id;
            if (isBlank(mid)) {
                mid = ids.claim(fieldToId(m.field));
                m = new Metric(mid, m.field, m.kind);
            }
            if (newMetrics.containsKey(mid)) {
                throw new IllegalArgumentException("Duplicate metric id: " + mid);
            }
            newMetrics.put(mid, m);
        }
        metrics = newMetrics;

        // ---------------- Ordering constraints ----------------
        Map<Integer, String> orderNeeds = new HashMap<Integer, String>();
        Map<Integer, String> orderDir = new HashMap<Integer, String>();

        for (int i = 0; i < termsList.size(); i++) {
            Terms t = termsList.get(i);
            if (!isBlank(t.orderMetricId)) {
                orderNeeds.put(i, t.orderMetricId);
                orderDir.put(i, isBlank(t.orderDir) ? "desc" : t.orderDir);
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
            String histId = hist.id;

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

        // build nested terms
        List<ObjectNode> termAggsContainers = new ArrayList<ObjectNode>();

        ObjectNode currentContainer = topAggsContainer;

        for (int i = 0; i < termsList.size(); i++) {
            Terms t = termsList.get(i);
            String termsId = t.id;

            ObjectNode termsNode = MAPPER.createObjectNode();
            ObjectNode termsObj = MAPPER.createObjectNode();

            termsObj.put("field", t.field);
            if (t.missing != null) {
                termsObj.put("missing", t.missing);
            }
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
            } else if (t.orderByKey) {
                String dir = isBlank(t.orderDir) ? "desc" : t.orderDir.trim().toLowerCase(Locale.ROOT);
                if (!"asc".equals(dir) && !"desc".equals(dir)) {
                    throw new IllegalArgumentException("Invalid order direction: " + dir);
                }
                ObjectNode ord = MAPPER.createObjectNode();
                ord.put("_key", dir);
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

    // ---------------- models ----------------
    private static final class Histogram {
        final String id, field, interval;
        final int minDocCount;
        Histogram(String id, String field, String interval, int minDocCount) {
            this.id = id; this.field = field; this.interval = interval; this.minDocCount = minDocCount;
        }
    }

    private static final class Terms {
        final String id, field, orderMetricId, orderDir, missing;
        final int size, minDocCount;
        final boolean orderByKey;
        Terms(String id, String field, int size, String orderMetricId, String orderDir, int minDocCount, String missing, boolean orderByKey) {
            this.id = id; this.field = field; this.size = size;
            this.orderMetricId = orderMetricId; this.orderDir = orderDir;
            this.minDocCount = minDocCount;
            this.missing = missing;
            this.orderByKey = orderByKey;
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
        String missing = args.get("missing");

        // order=id:11:desc  or  order=key:desc (order by the bucket's own term value)
        String order = args.get("order");
        String orderMetricId = null;
        String orderDir = "desc";
        boolean orderByKey = false;
        if (order != null && !order.trim().isEmpty()) {
            String o = order.trim();
            if (o.equals("key") || o.startsWith("key:")) {
                orderByKey = true;
                String[] parts = o.split(":", 2);
                if (parts.length >= 2) orderDir = parts[1].trim();
            } else if (o.startsWith("id:")) {
                String[] parts = o.split(":", 3);
                if (parts.length < 2) throw new IllegalArgumentException("terms.order missing metric id (order=id:11)");
                orderMetricId = parts[1].trim();
                if (orderMetricId.isEmpty()) throw new IllegalArgumentException("terms.order metric id is empty");
                if (parts.length >= 3) orderDir = parts[2].trim();
            } else {
                throw new IllegalArgumentException("terms.order must be like order=id:11, order=id:11:asc, order=key or order=key:asc");
            }
        }

        return new Terms(id, field, size, orderMetricId, orderDir, min, missing, orderByKey);
    }

    private static Metric parseMetric(String kind, Map<String, String> args) {
        String id = args.get("id"); // OPTIONAL
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

    private static boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }

    // Returns last segment after last dot: "span_attributes.score_ml" → "score_ml"; "max_score" → "max_score"
    private static String fieldToId(String field) {
        if (field == null || field.trim().isEmpty()) return "field";
        String f = field.trim();
        int dot = f.lastIndexOf('.');
        return dot >= 0 ? f.substring(dot + 1) : f;
    }

    // ---------------- id allocator ----------------
    private static final class IdAllocator {
        private final Set<String> used = new HashSet<String>();
        private int next = 1;

        void reserve(String id) {
            if (id == null) return;
            String s = id.trim();
            if (s.isEmpty()) return;
            if (!used.add(s)) throw new IllegalArgumentException("Duplicate id: " + s);
            try {
                int n = Integer.parseInt(s);
                if (n >= next) next = n + 1;
            } catch (NumberFormatException ignored) {}
        }

        // Try preferred name; fall back to numeric if already taken
        String claim(String preferred) {
            if (preferred != null && !preferred.trim().isEmpty()) {
                String s = preferred.trim();
                if (used.add(s)) return s;
                String fallback = allocate();
                logger.warn("Auto-ID collision: preferred id '{}' already taken; assigned '{}' instead. " +
                        "SQL column references to '{}' will break if data adds a new field with this name. " +
                        "Consider assigning explicit ids in the Aggs DSL.", s, fallback, s);
                return fallback;
            }
            return allocate();
        }

        String allocate() {
            while (used.contains(String.valueOf(next))) next++;
            String id = String.valueOf(next);
            used.add(id);
            next++;
            return id;
        }
    }

    // The development-only `main(String[])` demo was deleted in L06; its
    // `System.out.println` calls violated logging hygiene and the same DSL
    // shapes are now exercised by the AggsDslCompilerTest fixture set.
}
