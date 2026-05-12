package com.facebook.presto.quickwit;

import org.mozilla.javascript.ClassShutter;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.Scriptable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Isolated Rhino (JavaScript) execution helper with no Trino SPI dependency.
 *
 * Kept in a separate class so unit tests can load and call it on Java 24 without
 * triggering UnsupportedClassVersionError from trino-spi-479 (compiled for Java 25).
 *
 * L15: Thread Context Classloader (TCL) is saved before Context.enter() and
 * restored in the finally block. Trino's plugin classloader may not be the TCL;
 * without this fix Rhino falls back to the system classloader which cannot see
 * Rhino classes bundled inside the plugin JAR.
 */
final class RhinoExecutor {

    private static final Logger logger = LoggerFactory.getLogger(RhinoExecutor.class);
    static final int INSTRUCTION_LIMIT = 100_000;

    private RhinoExecutor() {}

    /**
     * Evaluates {@code script} in a sandboxed Rhino context and returns the
     * result as a String.  The script must evaluate to a String value.
     *
     * @throws RuntimeException if the script returns null, a non-String, or
     *         if the instruction limit is exceeded
     */
    static String executeScript(String script) {
        // Save and replace the Thread Context Classloader so Rhino's internal
        // ContextFactory can find its own classes inside the plugin JAR.
        ClassLoader savedTcl = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(RhinoExecutor.class.getClassLoader());
        Context cx = Context.enter();
        try {
            cx.setClassShutter(className -> false);  // R01: block all Java access from JS
            cx.setInstructionObserverThreshold(INSTRUCTION_LIMIT);
            Scriptable scope = cx.initSafeStandardObjects();
            Object result = cx.evaluateString(scope, script, "<cmd>", 1, null);
            if (result == null) {
                throw new RuntimeException("Rhino script returned null (script: "
                        + script.substring(0, Math.min(120, script.length())) + ")");
            }
            if (!(result instanceof String)) {
                throw new RuntimeException("Rhino script returned "
                        + result.getClass().getSimpleName() + " not String");
            }
            logger.debug("Rhino result: {}", result);
            return (String) result;
        }
        finally {
            Context.exit();
            Thread.currentThread().setContextClassLoader(savedTcl);
        }
    }
}
