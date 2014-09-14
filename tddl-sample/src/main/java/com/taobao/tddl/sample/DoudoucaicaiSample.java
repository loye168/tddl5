package com.taobao.tddl.sample;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.taobao.tddl.client.jdbc.TDataSource;
import com.taobao.tddl.util.SqlFileUtil;

public class DoudoucaicaiSample {

    public static void main(String[] args) throws Exception {

        TDataSource ds = new TDataSource();

        // init a datasource with dynamic config on diamond
        // ds.setSchemaFile("test_schema.xml");
        ds.setAppName("B2B_ICBU_AE_DATA_SHOW_DEV_APP");
        ds.setDynamicRule(true);

        // Map cp = new HashMap();
        // cp.put(ConnectionProperties.OPTIMIZER_CACHE_SIZE, 2000);
        // ds.setConnectionProperties(cp);
        ds.init();

        System.out.println("init done");

        Connection conn = ds.getConnection();
        String sql = SqlFileUtil.getSql("doudoucaicai1.sql");
        // PreparedStatement ps =
        // conn.prepareStatement("select sum(case when index_id=100013 then data_value else 1 end) pv,dimension_value countryId,sum(data_value) payGmv from adam_stream_sample_collect where index_id = 110008 and  date_time >='2014-02-25' and date_time <'2014-02-26' group by dimension_value  ");
        // // ps =
        // conn.prepareStatement("select collect.id as id, collect.gmt_create as gmtCreate,collect.gmt_modified as gmtModified,collect.index_id as indexId,collect.report_time as reportTime,collect.data_value as dataValue,collect.date_time as dateTime,collect.dimension_value as dimensionValue,collect.combine_key as combineKey,config.project_name as projectName,config.entity_name as entityName,config.date_time_type as dateTimeType,config.dimension_type as dimensionType,config.property_name as propertyName,config.is_monitor as isMonitor,config.is_discrete as isDiscrete,config.index_desc as indexDesc from adam_stream_sample_collect collect,adam_index_config config WHERE config.is_monitor = 'Y' and config.id = collect.index_id and collect.date_time >='2014-03-13' and collect.date_time < '2014-03-15'  and collect.report_time='2014-03-13 10:02:00'");
        PreparedStatement ps = conn.prepareStatement(sql);

        ps.setObject(1, "2014-03-21");
        ps.setObject(2, "2014-03-28");

        // Date minuteDate =
        // DateUtil.parseDate("2014-03-13 10:02:00","yyyy-MM-dd HH:mm:ss");
        // Map<String, Date> paramMap = new HashMap<String, Date>();
        // paramMap.put("reportDate", reportDate);
        // Date startDate = DateUtil.addDays(DateUtil.getZeroDate(reportDate),
        // -1);
        // Date endDate = DateUtil.addDays(DateUtil.getZeroDate(reportDate), 1);
        // paramMap.put("startDate", startDate);
        // paramMap.put("endDate", endDate);
        // PreparedStatement ps =
        // conn.prepareStatement("insert into adam_stream_sample_collect(id,gmt_create,gmt_modified,index_id,report_time,data_value,date_time,dimension_value,combine_key ) values(99999,'2014-03-05','2014-03-05',11,'2014-03-05',11,'2014-03-05','aaa','bb')");

        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            StringBuilder sb = new StringBuilder();
            int count = rs.getMetaData().getColumnCount();

            // System.out.println(rs.getBigDecimal("datavalue"));
            for (int i = 1; i <= count; i++) {

                String key = rs.getMetaData().getColumnLabel(i);
                Object val = rs.getObject(i);
                sb.append("[" + rs.getMetaData().getTableName(i) + "." + key + "->" + val + "]");
            }
            System.out.println(sb.toString());
        }

        rs.close();
        ps.close();
        conn.close();

        System.out.println("query done");

    }

}
