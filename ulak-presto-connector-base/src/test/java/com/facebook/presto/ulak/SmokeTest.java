package com.facebook.presto.ulak;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Sanity check that surefire + JUnit 5 + AssertJ are wired up. If this
 * fails to compile or run, the rest of the test suite cannot work.
 */
class SmokeTest
{
    @Test
    void junit_jupiter_assertj_are_wired()
    {
        assertThat(1 + 1).isEqualTo(2);
    }
}
