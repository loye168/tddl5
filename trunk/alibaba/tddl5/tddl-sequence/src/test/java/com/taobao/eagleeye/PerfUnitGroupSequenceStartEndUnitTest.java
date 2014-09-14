package com.taobao.eagleeye;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.ali.unit.rule.RouterTestHelper;
import com.ali.unit.rule.RouterUnitsListener.STATUS;
import com.taobao.tddl.client.sequence.Sequence;
import com.taobao.tddl.client.sequence.exception.SequenceException;
import com.taobao.tddl.client.sequence.impl.GroupSequence;
import com.taobao.tddl.client.sequence.impl.UnitGroupSequenceDao;
import com.taobao.tddl.group.jdbc.TGroupDataSource;

/**
 * @description
 * @author <a href="junyu@taobao.com">junyu</a>
 * @date 2013-5-27下午01:51:52
 */
@Ignore("手工跑,需要单独依赖tddl-client包")
public class PerfUnitGroupSequenceStartEndUnitTest {

    private Sequence             sequence;
    private static Set<String>   testUnits;
    private static String        currentUnit;
    private TGroupDataSource     s1;
    private TGroupDataSource     s2;
    private UnitGroupSequenceDao dao;

    public static final long     testStart = 30000000000l;

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

        dao = new UnitGroupSequenceDao();
        // 设置adjust为true,这样不必关心怎么样去设置有规律的值。单元化部署必须设置这个值为true,切换的时候自动更新
        dao.setAdjust(true);
        dao.setAppName("TDDLSEQUENCE");
        List<String> dbGroups = new ArrayList<String>();
        dbGroups.add("TDDL_SEQUENCE_GROUP0");
        dbGroups.add("TDDL_SEQUENCE_GROUP1");
        dao.setDbGroupKeys(dbGroups);
        dao.setDscount(2);
        dao.setInnerStep(1000);
        // 设定切换时候的零时表
        dao.setSwitchTempTable("sequence_temp");

        // 测试时先将主sequence表值设置下，具体使用时可能是应用最大的sequence id
        this.updateNormalTable(0);
        this.updateTestTable(testStart);

        testUnits = new HashSet<String>();
        testUnits.add("UNIT_CM3");
        testUnits.add("UNIT_XINGYI");
        testUnits.add("UNIT_DONGGUAN");
        testUnits.add("UNIT_CM9");
        currentUnit = "UNIT_CM9";

        // 设置测试参数
        RouterTestHelper.setTestData(testUnits, currentUnit, false);
        dao.init();

        // 有个名字叫ladygaga的sequence,自动初始化值
        GroupSequence s = new GroupSequence();
        s.setName("ladygaga");
        s.setSequenceDao(dao);
        s.init();
        sequence = s;
    }

    @Test
    public void test_nextValue() throws SequenceException {
        Set<Long> set = new HashSet<Long>();

        RpcContext_inner ctx = RpcContext_inner.get();
        if (ctx == null) {
            ctx = new RpcContext_inner(1);
            RpcContext_inner.set(ctx);
        }

        // 正常运行，取100000次seq
        for (int i = 0; i < 100000; i++) {
            Long id = sequence.nextValue();
            if (id >= testStart) {
                Assert.fail();
            }
            // System.out.println(id);
            boolean b = set.contains(id);
            Assert.assertFalse(b);
            set.add(id);

            ctx.putUserData("t", "1");

            try {
                Long id2 = sequence.nextValue();
                boolean b2 = set.contains(id2);
                Assert.assertFalse(b2);
                if (id2 < testStart) {
                    Assert.fail();
                }
                set.add(id2);
            } catch (SequenceException e) {
                e.printStackTrace();
                Assert.fail();
            }

            ctx.removeUserData("t");
        }

        testUnits = new HashSet<String>();
        testUnits.add("UNIT_CM3");
        testUnits.add("UNIT_XINGYI");
        testUnits.add("UNIT_DONGGUAN");
        testUnits.add("UNIT_CM9");
        testUnits.add("UNIT_CM6");

        // 准备扩容成5个unit,先更新temp sequence表比原表更大的值。
        this.updateTempTable(2000000);
        this.updateTestTempTable(testStart + 2000000);
        // 先切换到temp表
        RouterTestHelper.setListenerStatus(testUnits, currentUnit, STATUS.BEGIN, false);

        // 消耗掉原有的sequence
        for (int i = 0; i < 10000; i++) {
            Long id = sequence.nextValue();
            // System.out.println(id);
            boolean b = set.contains(id);
            Assert.assertFalse(b);
            set.add(id);
        }

        for (int i = 0; i < 10000; i++) {
            ctx.putUserData("t", "1");
            Long id = sequence.nextValue();
            ctx.removeUserData("t");
            // System.out.println(id);
            boolean b = set.contains(id);
            Assert.assertFalse(b);
            set.add(id);
        }

        // 这段sequence拿到的肯定大于2000000，因为已经到temp表上了
        for (int i = 0; i < 100000; i++) {
            Long id = sequence.nextValue();
            if (id < 2000000 || id > testStart) {
                Assert.fail();
            }

            // System.out.println(id);
            boolean b = set.contains(id);
            Assert.assertFalse(b);
            set.add(id);

            ctx.putUserData("t", "1");
            Long id2 = sequence.nextValue();
            ctx.removeUserData("t");
            if (id2 < testStart + 2000000) {
                Assert.fail();
            }

            // System.out.println(id);
            boolean b2 = set.contains(id2);
            Assert.assertFalse(b2);
            set.add(id2);
        }

        // 启用单元化完毕，需要切回到原sequence表，先更新到比temp sequence最大值的数据
        this.updateNormalTable(4000000);
        this.updateTestTable(testStart + 4000000);
        // 推送END标记
        RouterTestHelper.setListenerStatus(testUnits, currentUnit, STATUS.END, true);

        // 消耗掉从temp表拿到的seq
        for (int i = 0; i < 10000; i++) {
            Long id = sequence.nextValue();
            // System.out.println(id);
            boolean b = set.contains(id);
            Assert.assertFalse(b);
            set.add(id);
        }

        // 消耗掉从temp表拿到的seq
        for (int i = 0; i < 10000; i++) {
            ctx.putUserData("t", "1");
            Long id = sequence.nextValue();
            ctx.removeUserData("t");
            // System.out.println(id);
            boolean b = set.contains(id);
            Assert.assertFalse(b);
            set.add(id);
        }

        // 这个时候肯定切换到原sequence表，新拿到的seq值肯定比4000000大
        for (int i = 0; i < 100000; i++) {
            Long id = sequence.nextValue();
            if (id < 4000000 || id > testStart + 4000000) {
                Assert.fail();
            }

            // System.out.println(id);
            boolean b = set.contains(id);
            Assert.assertFalse(b);
            set.add(id);

            ctx.putUserData("t", "1");
            Long id2 = sequence.nextValue();
            ctx.removeUserData("t");
            if (id2 < testStart + 4000000) {
                Assert.fail();
            }

            // System.out.println(id);
            boolean b2 = set.contains(id2);
            Assert.assertFalse(b2);
            set.add(id2);
        }

        // 准备切回同城,先更新temp sequence表比原表更大的值。
        this.updateTempTable(6000000);
        this.updateTestTempTable(testStart + 6000000);
        // 先切换到temp表
        RouterTestHelper.setListenerStatus(testUnits, currentUnit, STATUS.BEGIN, true);

        // 消耗掉原有的sequence
        for (int i = 0; i < 10000; i++) {
            Long id = sequence.nextValue();
            // System.out.println(id);
            boolean b = set.contains(id);
            Assert.assertFalse(b);
            set.add(id);
        }

        for (int i = 0; i < 10000; i++) {
            ctx.putUserData("t", "1");
            Long id = sequence.nextValue();
            ctx.removeUserData("t");
            // System.out.println(id);
            boolean b = set.contains(id);
            Assert.assertFalse(b);
            set.add(id);
        }

        // 这段sequence拿到的肯定大于6000000，因为已经到temp表上了
        for (int i = 0; i < 100000; i++) {
            Long id = sequence.nextValue();
            if (id < 6000000 || id > testStart + 6000000) {
                Assert.fail();
            }

            // System.out.println(id);
            boolean b = set.contains(id);
            Assert.assertFalse(b);
            set.add(id);

            ctx.putUserData("t", "1");
            Long id2 = sequence.nextValue();
            ctx.removeUserData("t");
            if (id2 < testStart + 6000000) {
                Assert.fail();
            }

            // System.out.println(id);
            boolean b2 = set.contains(id2);
            Assert.assertFalse(b2);
            set.add(id2);
        }

        // 切回同城，需要切回到原sequence表，先更新到比temp sequence最大值的数据
        this.updateNormalTable(8000000);
        this.updateTestTable(testStart + 8000000);
        testUnits.clear();
        // 推送END标记
        RouterTestHelper.setListenerStatus(testUnits, currentUnit, STATUS.END, false);

        // 消耗掉从temp表拿到的seq
        for (int i = 0; i < 10000; i++) {
            Long id = sequence.nextValue();
            // System.out.println(id);
            boolean b = set.contains(id);
            Assert.assertFalse(b);
            set.add(id);
        }

        for (int i = 0; i < 10000; i++) {
            ctx.putUserData("t", "1");
            Long id = sequence.nextValue();
            ctx.removeUserData("t");
            // System.out.println(id);
            boolean b = set.contains(id);
            Assert.assertFalse(b);
            set.add(id);
        }

        // 这个时候肯定切换到原sequence表，新拿到的seq值肯定比4000000大
        for (int i = 0; i < 100000; i++) {
            Long id = sequence.nextValue();
            if (id < 8000000 || id > testStart + 8000000) {
                Assert.fail();
            }

            // System.out.println(id);
            boolean b = set.contains(id);
            Assert.assertFalse(b);
            set.add(id);

            ctx.putUserData("t", "1");
            Long id2 = sequence.nextValue();
            ctx.removeUserData("t");
            if (id2 < testStart + 8000000) {
                Assert.fail();
            }

            // System.out.println(id);
            boolean b2 = set.contains(id2);
            Assert.assertFalse(b2);
            set.add(id2);
        }
    }

    private void updateTempTable(long value) {
        String sql = "update " + dao.getSwitchTempTable() + " set " + dao.getValueColumnName() + "=" + value;
        update(sql);
    }

    private void updateTestTempTable(long value) {
        RpcContext_inner ctx = RpcContext_inner.get();
        if (ctx == null) {
            ctx = new RpcContext_inner(1);
            RpcContext_inner.set(ctx);
        }

        ctx.putUserData("t", "1");
        String sql = "update " + dao.getSwitchTempTable() + " set " + dao.getValueColumnName() + "=" + value;
        ctx.putUserData("t", "1");
        Assert.assertEquals("update __test_sequence_temp set value=" + value, sql);
        update(sql);
    }

    private void updateNormalTable(long value) {
        String sql = "update " + dao.getOriTableName() + " set " + dao.getValueColumnName() + "=" + value;
        update(sql);
    }

    private void updateTestTable(long value) {
        RpcContext_inner ctx = RpcContext_inner.get();
        if (ctx == null) {
            ctx = new RpcContext_inner(1);
            RpcContext_inner.set(ctx);
        }

        ctx.putUserData("t", "1");
        String sql = "update " + dao.getOriTableName() + " set " + dao.getValueColumnName() + "=" + value;
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
