package com.facebook.presto.ulak.geolocation;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.model.CountryResponse;
import com.maxmind.geoip2.record.Country;
import com.maxmind.geoip2.record.Location;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;

/**
 * Trino scalar UDFs for IP geolocation.
 *
 * <p>This product includes GeoLite2 data created by MaxMind, available from
 * <a href="https://www.maxmind.com">https://www.maxmind.com</a>.
 *
 * <p>This product uses IP2Location LITE data available from
 * <a href="https://lite.ip2location.com">https://lite.ip2location.com</a>.
 *
 * <p>The GeoIP database files are NOT bundled with this software (license-restricted).
 * They must be mounted at runtime. See {@code geolocation/README.md}.
 */
public class IPToCountry
{
    private static final Logger logger = LoggerFactory.getLogger(IPToCountry.class);

    private static final String DEFAULT_COUNTRY_DB = "/usr/lib/trino/plugin/GeoLite2-Country.mmdb";
    private static final String DEFAULT_CITY_DB = "/usr/lib/trino/plugin/GeoLite2-City.mmdb";

    private static final DatabaseReader countryReader = openReader(
            envOr("ULAK_GEOIP_COUNTRY_DB", DEFAULT_COUNTRY_DB), "country");
    private static final DatabaseReader cityReader = openReader(
            envOr("ULAK_GEOIP_CITY_DB", DEFAULT_CITY_DB), "city");

    private IPToCountry() {}

    private static String envOr(String var, String fallback)
    {
        String v = System.getenv(var);
        return (v != null && !v.isEmpty()) ? v : fallback;
    }

    private static DatabaseReader openReader(String path, String label)
    {
        File f = new File(path);
        if (!f.exists()) {
            logger.warn("GeoIP {} database not found at {} — ip_to_{} UDFs will return empty. "
                            + "Set ULAK_GEOIP_{}_DB or mount the MMDB. See geolocation/README.md.",
                    label, path, label, label.toUpperCase());
            return null;
        }
        try {
            return new DatabaseReader.Builder(f).build();
        }
        catch (IOException e) {
            logger.error("Failed to open GeoIP {} database at {} — UDFs will return empty.",
                    label, path, e);
            return null;
        }
    }

    private static String getCountryName(String ip)
    {
        if (countryReader == null || StringUtils.isBlank(ip)) {
            return null;
        }
        try {
            InetAddress ipAddress = InetAddress.getByName(ip);
            CountryResponse response = countryReader.country(ipAddress);
            Country country = response.country();
            return (country != null) ? country.name() : null;
        }
        catch (IOException | GeoIp2Exception e) {
            logger.debug("country lookup failed for {}: {}", ip, e.getMessage());
            return null;
        }
    }

    private static Location getLocation(String ip)
    {
        if (cityReader == null || StringUtils.isBlank(ip)) {
            return null;
        }
        try {
            InetAddress ipAddress = InetAddress.getByName(ip);
            CityResponse response = cityReader.city(ipAddress);
            return response.location();
        }
        catch (IOException | GeoIp2Exception e) {
            logger.debug("location lookup failed for {}: {}", ip, e.getMessage());
            return null;
        }
    }

    @ScalarFunction(value = "ip_to_country", deterministic = true)
    @Description("Returns a country name string if the ip is provided")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice ipToCountry(@SqlNullable @SqlType(StandardTypes.VARCHAR) Slice value)
    {
        if (value == null) {
            return Slices.utf8Slice("");
        }
        String name = getCountryName(value.toStringUtf8());
        return Slices.utf8Slice(name != null ? name : "");
    }

    @ScalarFunction(value = "ip_to_latitude", deterministic = true)
    @Description("Returns the latitude (string) if the ip is provided")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice ipToLatitude(@SqlNullable @SqlType(StandardTypes.VARCHAR) Slice value)
    {
        if (value == null) {
            return Slices.utf8Slice("");
        }
        Location loc = getLocation(value.toStringUtf8());
        Double lat = (loc != null) ? loc.latitude() : null;
        return Slices.utf8Slice(lat != null ? String.valueOf(lat) : "");
    }

    @ScalarFunction(value = "ip_to_longitude", deterministic = true)
    @Description("Returns the longitude (string) if the ip is provided")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice ipToLongitude(@SqlNullable @SqlType(StandardTypes.VARCHAR) Slice value)
    {
        if (value == null) {
            return Slices.utf8Slice("");
        }
        Location loc = getLocation(value.toStringUtf8());
        Double lon = (loc != null) ? loc.longitude() : null;
        return Slices.utf8Slice(lon != null ? String.valueOf(lon) : "");
    }
}
