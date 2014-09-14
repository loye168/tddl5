package com.taobao.tddl.sample;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import com.taobao.tddl.client.sequence.exception.SequenceException;
import com.taobao.tddl.client.sequence.impl.GroupSequence;
import com.taobao.tddl.client.sequence.impl.GroupSequenceDao;
import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.matrix.jdbc.TDataSource;

public class SequenceTest {

    static long kk = 0;

    public static void main(String args[]) throws SequenceException, SQLException, TddlException {

        final TDataSource ds = new TDataSource();

        // init a datasource with dynamic config on diamond
        ds.setAppName("tddl5_sample");

        Map cp = new HashMap();
        // cp.put(ConnectionProperties.OPTIMIZER_CACHE, "false");
        ds.setConnectionProperties(cp);
        ds.init();

        final Set<Long> used1 = new HashSet();
        final ReentrantLock lock1 = new ReentrantLock();

        final Set<Long> used2 = new HashSet();
        final ReentrantLock lock2 = new ReentrantLock();

        GroupSequenceDao sd = new GroupSequenceDao();
        sd.setAdjust(true);
        sd.setAppName("CBU_OCEAN_DEV_APP");
        List<String> groups = new ArrayList();
        groups.add("CBU_OCEAN_DEV_GROUP");
        sd.setDbGroupKeys(groups);
        sd.setDscount(1);
        sd.setTableName("ocean_sequence");
        sd.init();

        final GroupSequence sq1 = new GroupSequence();
        sq1.setSequenceDao(sd);
        sq1.setName("tuna_message");
        sq1.init();

        final GroupSequence sq2 = new GroupSequence();
        sq2.setSequenceDao(sd);
        sq2.setName("tuna_push_record");
        sq2.init();

        for (int i = 0; i < 10; i++) {
            new Thread(new Runnable() {

                public void run() {

                    while (true) {

                        if (kk++ % 100000 == 0) System.out.println(kk++);
                        Connection conn = null;
                        try {
                            long a = sq1.nextValue();

                            conn = ds.getConnection();
                            PreparedStatement ps = conn.prepareStatement("insert into sample_table (id,name,address) values (?,'sun','hz')");

                            ps.setLong(1, a);
                            ps.executeUpdate();
                            ps.close();
                            conn.close();
                        } catch (Exception e) {
                            e.printStackTrace();
                        } finally {
                            try {
                                conn.close();
                            } catch (SQLException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                            }
                        }
                    }

                }
            }).start();

            ;
        }

        // for (int i = 0; i < 20; i++) {
        // new Thread(new Runnable() {
        //
        // public void run() {
        //
        // while (true) {
        // try {
        // long a = sq2.nextValue();
        //
        // lock2.lock();
        // try {
        // if (used2.contains(a)) {
        // throw new RuntimeException(a + " ÖØ¸´ÁË");
        // }
        //
        // used2.add(a);
        // } finally {
        // lock2.unlock();
        // }
        // } catch (SequenceException e) {
        // e.printStackTrace();
        // }
        // }
        //
        // }
        // }).start();
        // }

    }
}
