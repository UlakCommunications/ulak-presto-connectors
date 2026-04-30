package com.facebook.presto.ulak;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Locks in the L06 credential-redaction helper. Drives
 * {@link QueryParameters#redactIfSecret(String, String)} via reflection
 * because the method is package-private.
 */
class QueryParametersRedactionTest
{
    @ParameterizedTest(name = "[{index}] {0} → redacted")
    @CsvSource({
            "RO_POSTGRES_PASSWORD",
            "DB_PASS",
            "PG_PWD",
            "API_TOKEN",
            "AWS_SECRET_ACCESS_KEY",
            "redis-credential",
            "Auth_Key"
    })
    void secret_like_names_are_redacted(String name)
            throws Exception
    {
        assertThat(invoke(name, "hunter2")).isEqualTo("***REDACTED***");
    }

    @ParameterizedTest(name = "[{index}] {0} → passthrough")
    @CsvSource({
            "QW_URL",
            "REDIS_URL",
            "PG_HOST",
            "WORKER_INDEX",
            "connecttimeout",
            "qwindex"
    })
    void non_secret_names_pass_through(String name)
            throws Exception
    {
        assertThat(invoke(name, "value-stays")).isEqualTo("value-stays");
    }

    @Test
    void null_or_empty_values_pass_through_even_for_secret_names()
            throws Exception
    {
        assertThat(invoke("PASSWORD", null)).isNull();
        assertThat(invoke("PASSWORD", "")).isEmpty();
    }

    private static String invoke(String name, String value)
            throws Exception
    {
        Method m = QueryParameters.class.getDeclaredMethod("redactIfSecret", String.class, String.class);
        m.setAccessible(true);
        return (String) m.invoke(null, name, value);
    }
}
