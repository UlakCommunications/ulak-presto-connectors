package com.facebook.presto.quickwit;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.URLClassLoader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * L15 — RhinoExecutor unit tests.
 *
 * Calls RhinoExecutor directly (no Trino SPI dep) so it runs on Java 24 without
 * UnsupportedClassVersionError from trino-spi-479.
 *
 * The TCL (Thread Context Classloader) test simulates what Trino's plugin
 * classloader environment does: the test deliberately sets a "wrong" TCL that
 * does not have Rhino on its classpath, then calls executeScript().
 * The fix inside RhinoExecutor must replace the TCL with the one that loaded
 * RhinoExecutor (which does have Rhino) so Math.floor and other stdlib calls work.
 */
@DisplayName("L15 — RhinoExecutor: Math.floor + classloader safety")
class RhinoExecutorTest {

    // -----------------------------------------------------------------------
    // Functional correctness
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("Math.floor returns integer string")
    void mathFloorReturnsCorrectValue() {
        // Reproduces the exact expression from the L15 bug report
        String result = RhinoExecutor.executeScript(
                "var a = String(Math.floor(1777551625/1000)); a");
        assertThat(result).isEqualTo("1777551");
    }

    @Test
    @DisplayName("basic arithmetic expression evaluates correctly")
    void basicArithmeticWorks() {
        String result = RhinoExecutor.executeScript(
                "var now = 1700000000; var d = 86400; String(now - d)");
        assertThat(result).isEqualTo("1699913600");
    }

    @Test
    @DisplayName("string concatenation returns correct value")
    void stringConcatenationWorks() {
        String result = RhinoExecutor.executeScript("'hello' + '-' + 'world'");
        assertThat(result).isEqualTo("hello-world");
    }

    // -----------------------------------------------------------------------
    // Security: ClassShutter blocks Java class access
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("ClassShutter blocks java.lang.Runtime access")
    void classShutterBlocksJavaRuntime() {
        assertThatThrownBy(() ->
                RhinoExecutor.executeScript("java.lang.Runtime.getRuntime().exec('id')"))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    @DisplayName("ClassShutter blocks java.io.File access")
    void classShutterBlocksFileAccess() {
        assertThatThrownBy(() ->
                RhinoExecutor.executeScript("new java.io.File('/etc/passwd').exists()"))
                .isInstanceOf(RuntimeException.class);
    }

    // -----------------------------------------------------------------------
    // L15: TCL save/restore under a "wrong" classloader
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("executeScript works even when TCL has no Rhino classes")
    void executeScriptWorksWithWrongTcl() {
        // Simulate Trino plugin classloader scenario: set TCL to an empty
        // classloader that has no Rhino on its classpath.
        ClassLoader emptyLoader = new URLClassLoader(new java.net.URL[0], null);
        ClassLoader orig = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(emptyLoader);
        try {
            // Our fix replaces TCL with RhinoExecutor's classloader internally,
            // so Math.floor must still work even with the wrong TCL set here.
            String result = RhinoExecutor.executeScript(
                    "var a = String(Math.floor(1777551625/1000)); a");
            assertThat(result).isEqualTo("1777551");
        } finally {
            Thread.currentThread().setContextClassLoader(orig);
        }
    }

    @Test
    @DisplayName("TCL is restored to the original value after execution")
    void tclIsRestoredAfterSuccess() {
        ClassLoader expected = Thread.currentThread().getContextClassLoader();
        RhinoExecutor.executeScript("String(1 + 1)");
        assertThat(Thread.currentThread().getContextClassLoader())
                .as("TCL must be restored after successful execution")
                .isSameAs(expected);
    }

    @Test
    @DisplayName("TCL is restored even when script throws")
    void tclIsRestoredAfterException() {
        ClassLoader expected = Thread.currentThread().getContextClassLoader();
        try {
            RhinoExecutor.executeScript("java.lang.Runtime.getRuntime()"); // will throw
        } catch (Exception ignored) {}
        assertThat(Thread.currentThread().getContextClassLoader())
                .as("TCL must be restored even after a script exception")
                .isSameAs(expected);
    }
}
