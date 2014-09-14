package com.taobao.eagleeye;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.taobao.tddl.client.sequence.Sequence;
import com.taobao.tddl.client.sequence.exception.SequenceException;
import com.taobao.tddl.client.sequence.impl.GroupSequence;
import com.taobao.tddl.client.sequence.impl.GroupSequenceDao;
import com.taobao.tddl.group.jdbc.TGroupDataSource;

@Ignore("手工跑,需要单独依赖tddl-client包")
public class PerfGroupSequenceDaoTest {

    private TGroupDataSource s1;
    private TGroupDataSource s2;
    private GroupSequenceDao dao;
    private Sequence         sequence;

    public static final long testStart = 10000000000l;

    @Before
    public void setUp() throws Exception {
        s1 = new TGroupDataSource();
        s1.setAppName("TDDLSEQUENCE");
        s1.setDbGroupKey("TDDL_SEQUENCE_GROUP0");
        s1.init();

        s2 = new TGroupDataSource();
        s2.setAppName("TDDLSEQUENCE");
        s2.setDbGroupKey("TDDL_SEQUENCE_GROUP1");
        s2.init();

        dao = new GroupSequenceDao();
        // 设置adjust为true,这样不必关心怎么样去设置有规律的值。单元化部署必须设置这个值为true,切换的时候自动更新
        dao.setAdjust(true);
        dao.setAppName("TDDLSEQUENCE");
        List<String> dbGroups = new ArrayList<String>();
        dbGroups.add("TDDL_SEQUENCE_GROUP0");
        dbGroups.add("TDDL_SEQUENCE_GROUP1");
        dao.setDbGroupKeys(dbGroups);
        dao.setDscount(2);
        dao.setInnerStep(1000);
        dao.init();

        updateNormalTable(0);
        updateTestTable(testStart);

        GroupSequence s = new GroupSequence();
        s.setName("ladygaga");
        s.setSequenceDao(dao);
        s.init();
        sequence = s;
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void test_nextRange() throws SequenceException, InterruptedException, ExecutionException {
        for (int i = 0; i < 200000; i++) {
            Future f = es.submit(new T1());
            Future f2 = es.submit(new T2());
            Future f3 = es.submit(new T2());
            Future f4 = es.submit(new T2());
            Future f5 = es.submit(new T1());
            Future f6 = es.submit(new T2());
            Future f7 = es.submit(new T1());
            Future f8 = es.submit(new T2());
            Future f9 = es.submit(new T1());
            Future f10 = es.submit(new T1());
            Future f11 = es.submit(new T1());
            Future f12 = es.submit(new T1());
            Future f13 = es.submit(new T1());
            Future f14 = es.submit(new T2());
            Future f15 = es.submit(new T2());
            Future f16 = es.submit(new T1());
            Future f17 = es.submit(new T2());
            Future f18 = es.submit(new T1());
            Future f19 = es.submit(new T2());
            Future f20 = es.submit(new T1());

            f.get();
            f2.get();
            f3.get();
            f4.get();
            f5.get();
            f6.get();
            f7.get();
            f8.get();
            f9.get();
            f10.get();
            f11.get();
            f12.get();
            f13.get();
            f14.get();
            f15.get();
            f16.get();
            f17.get();
            f18.get();
            f19.get();
            f20.get();
        }

        System.out.println("submit done");
        es.shutdown();
        System.out.println("done,set size " + set.size());
    }

    ExecutorService es  = Executors.newFixedThreadPool(20);
    final Set<Long> set = Collections.synchronizedSet(new HashSet<Long>());

    public class T1 implements Runnable {

        @Override
        public void run() {
            try {
                Long id = sequence.nextValue();
                boolean b = set.contains(id);
                Assert.assertFalse(b);
                Assert.assertTrue(id < testStart);
                if (id >= testStart) {
                    Assert.fail();
                }
                set.add(id);
            } catch (SequenceException e) {
                e.printStackTrace();
                Assert.fail();
            }
        }
    }

    public class T2 implements Runnable {

        @Override
        public void run() {
            RpcContext_inner ctx = RpcContext_inner.get();
            if (ctx == null) {
                ctx = new RpcContext_inner(1);
                RpcContext_inner.set(ctx);
            }

            ctx.putUserData("t", "1");

            try {
                Long id = sequence.nextValue();
                boolean b = set.contains(id);
                Assert.assertFalse(b);
                if (id < testStart) {
                    Assert.fail();
                }
                set.add(id);
            } catch (SequenceException e) {
                e.printStackTrace();
                Assert.fail();
            }

            ctx.removeUserData("t");
        }
    }

    private void updateNormalTable(long value) {
        String sql = "update " + dao.getTableName() + " set " + dao.getValueColumnName() + "=" + value;
        Assert.assertEquals("update sequence set value=" + value, sql);
        update(sql);
    }

    private void updateTestTable(long value) {
        RpcContext_inner ctx = RpcContext_inner.get();
        if (ctx == null) {
            ctx = new RpcContext_inner(1);
            RpcContext_inner.set(ctx);
        }

        ctx.putUserData("t", "1");
        String sql = "update " + dao.getTableName() + " set " + dao.getValueColumnName() + "=" + value;
        ctx.removeUserData("t");
        Assert.assertEquals("update __test_sequence set value=" + value, sql);
        update(sql);
    }

    private void update(String sql) {
        Connection conn = null;
        try {
            conn = s1.getConnection();
            Statement s = conn.createStatement();
            s.executeUpdate(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    // ignore;
                }
            }
        }

        try {
            conn = s2.getConnection();
            Statement s = conn.createStatement();
            s.executeUpdate(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    // ignore;
                }
            }
        }
    }
}
