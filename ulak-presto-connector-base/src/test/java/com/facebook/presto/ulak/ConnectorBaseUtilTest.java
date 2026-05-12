package com.facebook.presto.ulak;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Source-level structural tests for R23, R24, R31 fixes in
 * {@code ConnectorBaseUtil}.
 *
 * <p>Trino SPI JARs (trino-spi 479) are compiled at class-file version 69
 * (Java 25).  The build environment runs JDK 24 (max class-file version 68).
 * Loading {@code ConnectorBaseUtil} at runtime triggers eager resolution of
 * {@code ColumnMetadata} / {@code VarcharType} / {@code TrinoException},
 * all compiled at v69, causing {@code UnsupportedClassVersionError}.  We
 * therefore test the structural invariants by reading the source file as
 * text, following the same pattern used in {@code L04StructuralTest}.
 */
class ConnectorBaseUtilTest
{
    private static final Path SOURCE = Paths.get("src/main/java/com/facebook/presto/ulak/caching/ConnectorBaseUtil.java")
            .toAbsolutePath();

    private String src() throws IOException
    {
        return new String(Files.readAllBytes(SOURCE));
    }

    // -----------------------------------------------------------------------
    // R31: getColumnsBase(null) must throw TrinoException, not NullPointerException
    // -----------------------------------------------------------------------

    @Test
    void r31_getColumnsBase_throws_TrinoException_not_NullPointerException()
            throws IOException
    {
        String source = src();
        // After the fix, no bare NullPointerException must appear in the null-tables branch.
        // The fixed line is:
        //   throw new io.trino.spi.TrinoException(io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "Empty Query");
        assertThat(source)
                .as("getColumnsBase must not throw raw NullPointerException for null input")
                .doesNotContain("throw new NullPointerException(\"Empty Query\")");

        assertThat(source)
                .as("getColumnsBase must throw TrinoException for null input")
                .contains("TrinoException")
                .contains("Empty Query");
    }

    // -----------------------------------------------------------------------
    // R23: jedisPool and objectMapper must be declared volatile
    //      (double-checked locking requires volatile to be safe under JMM)
    // -----------------------------------------------------------------------

    @Test
    void r23_jedisPool_is_declared_volatile()
            throws IOException
    {
        String source = src();
        assertThat(source)
                .as("jedisPool must be declared volatile for safe double-checked locking")
                .contains("volatile JedisPool jedisPool");
    }

    @Test
    void r23_objectMapper_is_declared_volatile()
            throws IOException
    {
        String source = src();
        assertThat(source)
                .as("objectMapper must be declared volatile for safe double-checked locking")
                .contains("volatile ObjectMapper objectMapper");
    }

    // -----------------------------------------------------------------------
    // R24: arrangeCase must read keywords inside a synchronized block
    // -----------------------------------------------------------------------

    @Test
    void r24_arrangeCase_reads_keywords_inside_synchronized_block()
            throws IOException
    {
        String source = src();
        // The fixed arrangeCase uses a local variable assigned inside a
        // synchronized(inProgressLock) block.
        assertThat(source)
                .as("arrangeCase must use a synchronized block to read keywords")
                .contains("synchronized (inProgressLock)");
    }

    @Test
    void r24_arrangeCase_method_present_in_source()
            throws IOException
    {
        String source = src();
        assertThat(source)
                .as("arrangeCase method must still be present")
                .contains("public static String arrangeCase(");
    }

    // -----------------------------------------------------------------------
    // R33: logger field in ConnectorBaseUtil must be static final
    // -----------------------------------------------------------------------

    @Test
    void r33_logger_is_static_final_in_ConnectorBaseUtil()
            throws IOException
    {
        String source = src();
        assertThat(source)
                .as("logger must be declared private static final")
                .contains("private static final Logger logger");
        assertThat(source)
                .doesNotContain("private static Logger logger");
    }
}
