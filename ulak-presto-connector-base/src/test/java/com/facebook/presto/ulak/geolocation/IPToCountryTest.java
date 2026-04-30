package com.facebook.presto.ulak.geolocation;

import io.airlift.slice.Slices;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Behaviour test for {@link IPToCountry} UDFs.
 *
 * <p>The static block in {@code IPToCountry} resolves the MMDB path from
 * {@code ULAK_GEOIP_*_DB} env vars (defaults to
 * {@code /usr/lib/trino/plugin/GeoLite2-*.mmdb}). Under the test JVM
 * neither path is present, so {@code openReader} logs a {@code WARN} and
 * keeps both readers {@code null} — the UDFs return empty strings. That
 * is the K03 "graceful fallback" behaviour we explicitly want to lock in.
 *
 * <p>If a developer wants to extend these tests against a real MMDB,
 * point one of the env vars at
 * {@code geolocation/maxmind/GeoLite2-Country_20240917/GeoLite2-Country.mmdb}
 * before running {@code mvn test}.
 */
class IPToCountryTest
{
    @Test
    void ipToCountry_returns_empty_when_input_is_null()
    {
        assertThat(IPToCountry.ipToCountry(null).toStringUtf8()).isEmpty();
    }

    @Test
    void ipToCountry_returns_empty_when_input_is_blank()
    {
        assertThat(IPToCountry.ipToCountry(Slices.utf8Slice("")).toStringUtf8()).isEmpty();
        assertThat(IPToCountry.ipToCountry(Slices.utf8Slice("   ")).toStringUtf8()).isEmpty();
    }

    @Test
    void ipToCountry_returns_empty_for_non_ip_string()
    {
        // Without an MMDB the reader is null and we short-circuit; with one
        // present the InetAddress.getByName would throw and the catch
        // returns null → "" via Slices. Either way: no crash, empty string.
        assertThat(IPToCountry.ipToCountry(Slices.utf8Slice("not-an-ip")).toStringUtf8()).isEmpty();
    }

    @Test
    void ipToLatitude_returns_empty_when_input_is_null()
    {
        assertThat(IPToCountry.ipToLatitude(null).toStringUtf8()).isEmpty();
    }

    @Test
    void ipToLatitude_returns_empty_when_input_is_blank()
    {
        assertThat(IPToCountry.ipToLatitude(Slices.utf8Slice("")).toStringUtf8()).isEmpty();
    }

    @Test
    void ipToLongitude_returns_empty_when_input_is_null()
    {
        assertThat(IPToCountry.ipToLongitude(null).toStringUtf8()).isEmpty();
    }

    @Test
    void ipToLongitude_returns_empty_when_input_is_blank()
    {
        assertThat(IPToCountry.ipToLongitude(Slices.utf8Slice("")).toStringUtf8()).isEmpty();
    }

    @Test
    void udfs_never_return_null()
    {
        // Trino UDFs returning null cause type-checker grief downstream;
        // K03 guarantees we always hand back a Slice.
        assertThat(IPToCountry.ipToCountry(Slices.utf8Slice("8.8.8.8"))).isNotNull();
        assertThat(IPToCountry.ipToLatitude(Slices.utf8Slice("8.8.8.8"))).isNotNull();
        assertThat(IPToCountry.ipToLongitude(Slices.utf8Slice("8.8.8.8"))).isNotNull();
    }
}
