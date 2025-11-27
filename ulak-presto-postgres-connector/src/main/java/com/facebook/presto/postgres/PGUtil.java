/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.presto.postgres;

import com.facebook.presto.ulak.DBType;
import com.facebook.presto.ulak.QueryParameters;
import com.facebook.presto.ulak.UlakRow;
import com.facebook.presto.ulak.caching.ConnectorBaseUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.dbcp.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.*;

import static com.facebook.presto.ulak.QueryParameters.replaceAll;

public class PGUtil {

    private static Logger logger = LoggerFactory.getLogger(PGUtil.class);
    private static Map<String, BasicDataSource> dbcpDataSources = null;

    public static BasicDataSource getPool(String pgUrl,String pgUser,String pgPwd) {
        if (pgUrl == null) {
            return null;
        }
        if (dbcpDataSources == null) {
            dbcpDataSources =  new LinkedHashMap<>();

        }
        BasicDataSource  dbcpDataSource = null;
        if (!dbcpDataSources.containsKey(pgUrl)) {
            dbcpDataSource = new BasicDataSource();
            dbcpDataSource.setUrl(pgUrl);
            dbcpDataSource.setUsername(pgUser);
            dbcpDataSource.setPassword(pgPwd);
            dbcpDataSource.setMinIdle(20);
            dbcpDataSource.setMaxIdle(30);
            dbcpDataSource.setMaxOpenPreparedStatements(128);
            dbcpDataSources.put(pgUrl, dbcpDataSource);
        }else {
            dbcpDataSource = dbcpDataSources.get(pgUrl);
        }
        return dbcpDataSource;
    }

    private PGUtil() {
    }

    public static List<String> getSchemas(QueryParameters queryParameters,String pgUrl, String pgUser, String pgPwd) throws SQLException, JsonProcessingException {
        logger.debug("getSchemas");
        logger.debug("PGUtil-getSchemas");
        List<String> res = new ArrayList<>();
        List<UlakRow> rows = executeOneQuery(queryParameters, "select schema_name from information_schema.schemata",  pgUrl,   pgUser,   pgPwd);
        for (UlakRow bucket1 : rows) {
            String schemaName = (String) bucket1.getColumnMap().get("schema_name");
            res.add(schemaName);
            logger.debug(schemaName);
        }
        return res;
    }

    public static List<String> getTableNames(QueryParameters queryParameters,String schema,String   pgUrl,   String  pgUser,   String  pgPwd) throws SQLException, JsonProcessingException {
        logger.debug("PGUtil- bucket->tableNames: {}", schema);
        List<String> res = new ArrayList<>();
        List<UlakRow> rows = executeOneQuery(queryParameters, "SELECT table_name\n" +
                "  FROM information_schema.tables\n" +
                " WHERE table_schema='"+ schema +"'\n" +
                "   AND table_type='BASE TABLE'",  pgUrl,   pgUser,   pgPwd);
        for (UlakRow bucket1 : rows) {
            String schemaName = (String) bucket1.getColumnMap().get("table_name");
            res.add(schemaName);
            logger.debug(schemaName);
        }
        return res;
    }


    public static List<UlakRow> select(String tableName ,String   pgUrl,   String  pgUser,   String  pgPwd) throws IOException, SQLException {
        QueryParameters queryParameters = QueryParameters.getQueryParameters(tableName);
        return select(queryParameters,  pgUrl,   pgUser,   pgPwd);
    }

    public static List<UlakRow> select(QueryParameters queryParameters,String   pgUrl,   String  pgUser,   String  pgPwd) throws IOException, SQLException {
        queryParameters.setDbType(DBType.PG);
        queryParameters.setStart(System.currentTimeMillis());

        queryParameters.setError("");
        String query = queryParameters.getQuery();//"from(bucket: " + "\"" + bucket + "\"" + ")\n" + "|> range(start:" + time_interval + ")\n" + "|> filter(fn : (r) => r._measurement == " + "\"" + tableName + "\"" + ")";

        List<UlakRow> ret = executeOneQuery(queryParameters, query,  pgUrl,   pgUser,   pgPwd);
//                    addOneStat(hash, 1);
        return ret ;

    }

    private static List<UlakRow> executeOneQuery(QueryParameters queryParameters, String query, String pgUrl, String pgUser, String pgPwd) throws SQLException, JsonProcessingException {
        BasicDataSource dbPool = getPool(pgUrl,   pgUser,   pgPwd);
        try (Connection connection = dbPool.getConnection()) {
            try (Statement statement = connection.createStatement()) {
                ArrayList<UlakRow> list = new ArrayList<>();
                logger.debug("Running: {}",query);
                try (ResultSet tables = statement.executeQuery(query)) {
                    ResultSetMetaData rsmd = tables.getMetaData();
                    while (tables.next()) {
                        Map<String, Object> newRow = new HashMap<>();
                        for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                            newRow.put(rsmd.getColumnName(i), tables.getString(i));
                        }
                        list.add(new UlakRow(newRow));
                    }
                    if(list.isEmpty()) {
                        String[] columns = queryParameters.getColumns();
                        Map<String, Object> newRow = new HashMap<>();
                        if (columns != null) {
                            for (int i = 0; i < columns.length; i++) {
                                newRow.put(columns[i], null);
                            }
                            list.add(new UlakRow(newRow));
                        }
                    }
                    return list;
                }
            }
        } catch (NullPointerException e) {
            throw new NullPointerException(e.toString());
        }
    }

    public static void main(String[] args)    {
        long start = System.currentTimeMillis();
        QueryParameters params = QueryParameters.getQueryParameters(" \n" +
                "select i.*\n" +
                "from maya_grafana.otlp_metric.\"\n" +
                "--ttl=150\n" +
                "--refresh=75\n" +
                "--cache=false\n" +
                "--columns=alarm_name,silenced,alert_time,plugin_instance,type_instance,alarm_count,last_status,evaluate_status,title,type,m_iface,currentvalue,site_name_from_alert,datasource,alert_time_since,alarm_description,host,m_overlay,sensor,resolve_count,alarm_id,time,nodata_count,notification_description\n" +
                "--dbtype=pg\n" +
                "\n" +
                "with atmp as (\n" +
                "select rule_uid,\n" +
                "\t\tcurrent_state,\n" +
                "\t\tlabels_hash,\n" +
                "\t\ttrim(both '\"\"' from (jsonb_array_elements(ai.labels::jsonb)->0)::text) as k,\n" +
                "\t\ttrim(both '\"\"' from (jsonb_array_elements(ai.labels::jsonb)->1)::text) as v\n" +
                "from alert_instance ai\n" +
                "),silence as (\n" +
                "\t\tselect distinct *,\n" +
                "\t\t\t\ttrim(both '\"\"' from ((ms.status::jsonb)->'state')::text) as state ,\n" +
                "\t\t\t\ttrim(both '\"\"' from (a.value::text)) as silenced\n" +
                "\t\tfrom maya_silence ms\n" +
                "\t\tleft join jsonb_array_elements(ms.silenced_alerts::jsonb) a on true\n" +
                "),anno_tmp as (\n" +
                "\tselect *\n" +
                "\tfrom annotation a\n" +
                ")\n" +
                "\n" +
                "select * from (\n" +
                "    select  case when lower(atmp10.v) = 'failure' then 'Alerting'\n" +
                "    \t\t\twhen lower(atmp10.v) = 'warning' then 'Warning'\n" +
                "    \t\t\telse ai.current_state end  evaluate_status,\n" +
                "    \t\tatmp10.v  last_status,\n" +
                "                -- replace(replace( (regexp_matches (ai.labels, '\\\"\"severity\"\",\"\".*?\"\"]'))[1] ,'\"\"severity\"\",\"\"',''),'\"\"]','')  as severity,\n" +
                "              ar.title ,\n" +
                "                atmp1.v as sensor,\n" +
                "                atmp2.v  as host,\n" +
                "                coalesce(atmp3.v,ar.title)  as alarm_name,\n" +
                "                atmp4.v  as plugin_instance,\n" +
                "                atmp5.v  as type,\n" +
                "                atmp6.v  as type_instance,\n" +
                "                atmp7.v  as datasource,\n" +
                "                atmp8.v  as currentvalue,\n" +
                "                atmp11.v  as notification_description, \n" +
                "                atmp12.v  as site_name_from_alert, \n" +
                "                atmp13.v  as m_overlay,\n" +
                "                atmp14.v  as m_iface,\n" +
                "                case when coalesce(atmp3.v,ar.title)='operational_status' then 'Control Plane Down'\n" +
                "\t\t            when  coalesce(atmp3.v,ar.title)='maya_temperature' then 'System Interval Temperature'\n" +
                "\t\t    \t\twhen coalesce(atmp3.v,ar.title)= 'metric_absence'  then 'Monitoring agent down'\n" +
                "\t\t    \t\twhen coalesce(atmp3.v,ar.title)= 'sdnc_node_cpu'  then 'SDNC Node Cpu High'\n" +
                "\t\t    \t\twhen coalesce(atmp3.v,ar.title)= 'sdnc_node_ram'  then 'SDNC Node Memory High'\n" +
                "\t\t    \t\twhen coalesce(atmp3.v,ar.title)= 'sdnc_node_disk'  then 'SDNC Node Disk High'\n" +
                "\t\t            when  coalesce(atmp3.v,ar.title)='maya_ifstatus' then 'Interface Down'\n" +
                "\t\t            when  coalesce(atmp3.v,ar.title)='maya_bfd' then 'Data Plane Down'\n" +
                "\t\t            when  coalesce(atmp3.v,ar.title)='df' then 'High Disk Usage'\n" +
                "\t\t            when  coalesce(atmp3.v,ar.title)='cpu' then 'CPU Utilization Exceeded'\n" +
                "\t\t            when  coalesce(atmp3.v,ar.title)='alert_licence' then 'Alert Licence'\n" +
                "\t\t            when  coalesce(atmp3.v,ar.title)='memory' then 'RAM Utilization Exceeded'\n" +
                "\t\t            when  coalesce(atmp3.v,ar.title)='maya_probe' and atmp7.v='avg_delay' then 'Delay'\n" +
                "\t\t            when  coalesce(atmp3.v,ar.title)='maya_probe' and atmp7.v='jitter' then 'Jitter'\n" +
                "\t\t            when  coalesce(atmp3.v,ar.title)='maya_probe' and atmp7.v='loss_percentage' then 'Packet Loss'\n" +
                "\t\t            when  coalesce(atmp3.v,ar.title)='maya_system_services' then 'Service Down'\n" +
                "\t\t\t\t\twhen  coalesce(atmp3.v,ar.title)='maya_dhcp_relay' then 'DHCP Server Unreachable'\n" +
                "\t\t\t\t\twhen  coalesce(atmp3.v,ar.title)='maya_dhcp' then 'DHCP Lease Pool Exhaustion'\n" +
                "\t\t\t\t\twhen  coalesce(atmp3.v,ar.title)='maya_bgp' then 'BGP Peer Down'\n" +
                "\t\t        else coalesce(atmp3.v,ar.title) end alarm_description,\n" +
                "            trim(both '\"\"' from (ar.annotations::jsonb->'alarm_id')::text)alarm_id,\n" +
                "            TO_TIMESTAMP(ai.last_eval_time)  time,\n" +
                "            TO_TIMESTAMP(coalesce(cast(atmp9.v as float)/1000000000 , cast(ai.current_state_since as float)))  alert_time,\n" +
                "\t\t\tround( extract(epoch from (now() - TO_TIMESTAMP(coalesce(cast(atmp9.v as float)/1000000000, cast(ai.current_state_since as float))))) / 3600, 0) as alert_time_since,\n" +
                "            (\n" +
                "                select case when silence is null  then 0 else 1 end  silence\n" +
                "                from (\n" +
                "                select 1 as silence from silence   where lower(silence.state) = 'active' and silence.silenced = ai.labels_hash order by id desc  limit 1\n" +
                "                ) as q1\n" +
                "            )  as silenced,\n" +
                "            --            (select 1 from silence   where lower(silence.state) = 'active' and silence.silenced = ai.labels_hash  order by silence.id desc  limit 1)  as silenced,\n" +
                "            --\t\t(select count(silence_history.id) from silence_history   where lower(silence_history.state) = 'active' and silence_history.silenced = ai.labels_hash )  as silenced_count,\n" +
                "            --TODO: postgres have LAST aggregation?\n" +
                "            (select 1 from (select lower(b.prev_state)prev_state,lower(b.new_state)new_state from anno_tmp b where b.alert_id = ar.id order by b.id desc limit 1) a where lower(a.prev_state)='alerting'  and lower(new_state)='normal') as resolve_count,\n" +
                "            case when lower(ai.current_state) = 'alerting' then 1 else 0 end  as alarm_count,\n" +
                "            case when lower(ai.current_state) = 'nodata' then 1 else 0 end  as nodata_count\n" +
                "    from alert_instance ai\n" +
                "    left join alert_rule ar  on ai.rule_uid = ar.uid\n" +
                "    left join maya_silence ms on ms.status like '%\"\"active\"\"%' and ms.silenced_alerts like '%' || ai.labels_hash || '%' \n" +
                "    left join atmp atmp1  on atmp1.k = 'grafana_folder' and atmp1.rule_uid = ai.rule_uid  and atmp1.labels_hash = ai.labels_hash\n" +
                "    left join atmp atmp2  on atmp2.k = 'host'           and atmp2.rule_uid = ai.rule_uid  and atmp2.labels_hash = ai.labels_hash\n" +
                "    left join atmp atmp3  on atmp3.k = 'm_notif_plugin' and atmp3.rule_uid = ai.rule_uid  and atmp3.labels_hash = ai.labels_hash\n" +
                "    left join atmp atmp4  on atmp4.k = 'm_notif_plugin_instance' and atmp4.rule_uid = ai.rule_uid  and atmp4.labels_hash = ai.labels_hash\n" +
                "    left join atmp atmp5  on atmp5.k = 'm_notif_type' and atmp5.rule_uid = ai.rule_uid  and atmp5.labels_hash = ai.labels_hash\n" +
                "    left join atmp atmp6  on atmp6.k = 'm_notif_type_instance' and atmp6.rule_uid = ai.rule_uid  and atmp6.labels_hash = ai.labels_hash\n" +
                "    left join atmp atmp7  on atmp7.k = 'm_datasource' and atmp7.rule_uid = ai.rule_uid  and atmp7.labels_hash = ai.labels_hash\n" +
                "    left join atmp atmp8  on atmp8.k = 'm_currentvalue' and atmp8.rule_uid = ai.rule_uid  and atmp8.labels_hash = ai.labels_hash\n" +
                "    left join atmp atmp9  on atmp9.k = 'max_date_num' and atmp9.rule_uid = ai.rule_uid  and atmp9.labels_hash = ai.labels_hash\n" +
                "    left join atmp atmp10  on atmp10.k = 'status' and atmp10.rule_uid = ai.rule_uid  and atmp10.labels_hash = ai.labels_hash\n" +
                "    left join atmp atmp11  on atmp11.k = 'notification_a' and atmp11.rule_uid = ai.rule_uid  and atmp11.labels_hash = ai.labels_hash\n" +
                "    left join atmp atmp12  on atmp12.k = 'site_name_a' and atmp12.rule_uid = ai.rule_uid  and atmp12.labels_hash = ai.labels_hash\n" +
                "    left join atmp atmp13  on atmp13.k = 'm_overlay' and atmp13.rule_uid = ai.rule_uid  and atmp13.labels_hash = ai.labels_hash\n" +
                "    left join atmp atmp14  on atmp14.k = 'm_iface' and atmp14.rule_uid = ai.rule_uid  and atmp14.labels_hash = ai.labels_hash\n" +
                "    where lower(ai.current_state) != 'pending'  and lower(ai.current_state) != 'normal'  and ms.id is null\n" +
                ") s \n" +
                " \" as i\n" +
                " \n" +
                " \n" +
                " \n" +
                " \n" +
                " \n" +
                "  ".toLowerCase());

        params.setQuery(replaceAll(params.getQuery(),"|"," "));
        params.setQuery(replaceAll(params.getQuery()," not "," NOT "));
        params.setQuery(replaceAll(params.getQuery(),":IN [*]",":*"));
        params.setQuery(replaceAll(params.getQuery(),":IN [-]",":*"));
        params.setQwIndex("flows3");
        params.setDbType(DBType.QW);
        params.setQwUrl("http://10.20.4.53:32215");
        params.setReplaceFromColumns("/3/buckets/2/buckets/4/buckets/5/buckets/1");
        params.setHasJs(false);
        params.setToBeCached(false);
        List<UlakRow> ret = null;
        try {
            ret = ConnectorBaseUtil.select(params,
                    false,new String[]{params.getQwUrl(), params.getQwIndex()}, (q, s)-> {
                        try {
//                                logger.debug("From UlakQuickwitMetadata getTableMetadata: {}\n\n\nurl:{}\n\n\nindex:{}",
//                                        q.getQuery(),
//                                        s[0],
//                                        s[1]);
                            return  PGUtil.select(q , s[0], s[1], s[3]);
                        } catch (IOException | SQLException e) {
                            logger.error("ERRORSTRING", e);
                            throw new RuntimeException(e);
                        }
                    });
//            ret = Lists.newArrayList(QwUtil.select(params,params.getQwUrl(), params.getQwIndex()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        logger.info(String.valueOf(System.currentTimeMillis() - start));

    }
}
