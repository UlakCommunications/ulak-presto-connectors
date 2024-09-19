package com.facebook.presto.ulak.geolocation;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CountryResponse;
import com.maxmind.geoip2.record.Country;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceUtf8;
import io.airlift.slice.Slices;
import io.trino.spi.function.*;
import io.trino.spi.type.StandardTypes;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;


public class IPToCountry {
    private static DatabaseReader reader = null;

    private static Logger logger = LoggerFactory.getLogger(IPToCountry.class);
    static {

        // A File object pointing to your GeoIP2 or GeoLite2 database
        File database = new File("/usr/lib/trino/plugin/GeoLite2-Country.mmdb");

// This creates the DatabaseReader object. To improve performance, reuse
// the object across lookups. The object is thread-safe.
        reader = null;
        try {
            reader = new DatabaseReader.Builder(database).build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        //getCountryName("128.101.101.101");
//        System.out.println(country.getIsoCode());            // 'US'
//        System.out.println(country.getName());               // 'United States'
//        System.out.println(country.getNames().get("zh-CN")); // '美国'

//        Subdivision subdivision = response.getMostSpecificSubdivision();
//        System.out.println(subdivision.getName());    // 'Minnesota'
//        System.out.println(subdivision.getIsoCode()); // 'MN'
//
//        City city = response.getCity();
//        System.out.println(city.getName()); // 'Minneapolis'
//
//        Postal postal = response.getPostal();
//        System.out.println(postal.getCode()); // '55455'
//
//        Location location = response.getLocation();
//        System.out.println(location.getLatitude());  // 44.9733
//        System.out.println(location.getLongitude()); // -93.2323
    }

    private static String getCountryName(String ip) throws IOException, GeoIp2Exception {
        InetAddress ipAddress = null;
        ipAddress = InetAddress.getByName(ip);

// Replace "city" with the appropriate method for your database, e.g.,
// "country".
        CountryResponse response = null;
        response = reader.country(ipAddress);

        Country country = response.getCountry();
        return country.getName();
    }
//    @TypeParameter("T")
//    @SqlType(StandardTypes.VARCHAR)
//    public static String isNullSlice(@SqlNullable @SqlType("T") Slice value)
//    {
//        return "";
//    }

//    @ScalarFunction("ip_to_country")
//    @Description("Returns a country string if the ip is provided")
//    @SqlType(StandardTypes.VARCHAR)
//    public static String ipToCountry(@SqlNullable  @SqlType(StandardTypes.VARCHAR) String value)
//    {
//        if(StringUtils.isNotBlank(value)) {
//            return getCountryName(value);
//        }
//        return null;
//    }

    @ScalarFunction(value="ip_to_country", deterministic = true)
    @Description("Returns a country string if the ip is provided")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice ipToCountry(
            @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice string)
    {
        if(StringUtils.isNotBlank(string.toStringUtf8())) {

            try {
                return Slices.utf8Slice((getCountryName(string.toStringUtf8())));
            } catch (IOException e) {
                logger.error("Communication error",e);
            } catch (GeoIp2Exception e) {
                logger.error("GeoIp2Exception",e);
            }
        }
        return Slices.utf8Slice("");
    }
//    @TypeParameter("T")
//    @SqlType(StandardTypes.VARCHAR)
//    public static String isNullDouble(@SqlNullable @SqlType("T") Double value)
//    {
//        return "";
//    }

}
