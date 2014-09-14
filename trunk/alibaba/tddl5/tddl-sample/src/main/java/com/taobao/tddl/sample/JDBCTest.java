package com.taobao.tddl.sample;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.alibaba.druid.pool.DruidDataSource;

public class JDBCTest {

    public static void main(String[] args) throws Exception {
        DruidDataSource ds = new DruidDataSource();
        ds.setUrl("jdbc:mysql://42.120.217.1/DRDS_1053599184310491_HOIBS_CLUSTER?characterEncoding=utf8");
        ds.setUsername("DRDS_1053599184310491_HOIBS_CLUSTER");
        ds.setPassword("hfd000999");

        Connection conn = ds.getConnection();
        conn.prepareStatement("set names utf8mb4").executeUpdate();

        byte[] b3 = { (byte) 0xF0, (byte) 0x9F, (byte) 0x90, (byte) 0xA0 }; // 0xF0
                                                                            // 0x9F
                                                                            // 0x8F
                                                                            // 0x80

        String text = new String(b3, "utf-8");
        text.getBytes("utf-8");
        PreparedStatement ps = conn.prepareStatement("insert into INS_EBAY_MESSAGE(msg_id,deadline_time,folder,item_end_time,item_no,message_no,is_read,receive_time,recipient_usr_id,sender,send_to_name,subject,create_time,order_id,usr_id,text) values(?,'2015-03-24 16:37:16','0','2014-02-23 02:42:29','181330498504','54529786058','true','2014-03-24 16:37:16','spring.inc','babz.brinz','spring.inc','babz.brinzClearForAppleiPhone55G5SHotSale0.5mmUltraThinMatteBackCaseSkin','2014-06-30 18:19:26',7001,4010,?);");
        long msg_id = System.currentTimeMillis();
        ps.setLong(1, msg_id);
        ps.setString(2, text);
        ps.executeUpdate();

        ps = conn.prepareStatement("select * from INS_EBAY_MESSAGE where msg_id=?");
        ps.setLong(1, msg_id);

        ResultSet rs = ps.executeQuery();

        while (rs.next()) {
            System.out.println(rs.getString("text").getBytes());
        }
        ps.close();
        conn.close();
        System.out.println("query done");
    }
}
