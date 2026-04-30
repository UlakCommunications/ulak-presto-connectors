package com.facebook.presto.ulak;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Source-level regression test for the L04 multi-catalog fix.
 *
 * <p>The classes covered by L04 directly implement Trino SPI 479
 * interfaces (class-file v69 / Java 25); instantiating them on the
 * local JDK 24 build throws {@code UnsupportedClassVersionError}, so a
 * behaviour test (see {@code UlakQuickwitMetadataMultiCatalogTest})
 * is {@code @Disabled} until CI runs on JDK 25.
 *
 * <p>Until then, this test guards the structural invariant by reading
 * the source file as text and asserting that the
 * {@code private static <T> single} +
 * {@code public static <T> getInstance(...)} pattern is gone. If it
 * comes back, the multi-catalog bug comes back, and this test fails.
 */
class L04StructuralTest
{
    private static final Path REPO_ROOT = Paths.get("..").toAbsolutePath().normalize();

    private static final Pattern SINGLE_FIELD = Pattern.compile(
            "(?m)^\\s*(private|public|protected)?\\s*static\\s+\\w[\\w<>,\\s\\?]*\\s+single\\s*;");
    private static final Pattern GET_INSTANCE = Pattern.compile(
            "(?m)^\\s*public\\s+static\\s+\\w[\\w<>,\\s\\?]*\\s+getInstance\\s*\\(");

    @ParameterizedTest(name = "{0} has no `static single` and no `getInstance(...)`")
    @CsvSource({
            "ulak-presto-connector-base/src/main/java/com/facebook/presto/ulak/UlakSplitManager.java",
            "ulak-presto-connector-base/src/main/java/com/facebook/presto/ulak/UlakRecordSetProvider.java",
            "ulak-presto-quickwit-connector/src/main/java/com/facebook/presto/quickwit/UlakQuickwitMetadata.java",
            "ulak-presto-quickwit-connector/src/main/java/com/facebook/presto/quickwit/QuickwitSplitManager.java",
            "ulak-presto-quickwit-connector/src/main/java/com/facebook/presto/quickwit/QuickwitRecordSetProvider.java"
    })
    void no_static_singleton_pattern(String relativePath)
            throws Exception
    {
        Path source = REPO_ROOT.resolve(relativePath);
        assertThat(source).as("source file exists").isRegularFile();

        String text = new String(Files.readAllBytes(source));
        assertThat(SINGLE_FIELD.matcher(text).find())
                .as("`static <T> single` field still present in %s — L04 regression", relativePath)
                .isFalse();
        assertThat(GET_INSTANCE.matcher(text).find())
                .as("`public static getInstance(...)` still present in %s — L04 regression", relativePath)
                .isFalse();
    }

    @Test
    void quickwit_connector_constructs_metadata_with_new()
            throws Exception
    {
        Path source = REPO_ROOT.resolve(
                "ulak-presto-quickwit-connector/src/main/java/com/facebook/presto/quickwit/UlakQuickwitConnector.java");
        String text = new String(Files.readAllBytes(source));
        assertThat(text).contains("new UlakQuickwitMetadata(");
        assertThat(text).contains("new QuickwitSplitManager(");
        assertThat(text).contains("new QuickwitRecordSetProvider(");
        assertThat(text).doesNotContain(".getInstance(");
    }
}
