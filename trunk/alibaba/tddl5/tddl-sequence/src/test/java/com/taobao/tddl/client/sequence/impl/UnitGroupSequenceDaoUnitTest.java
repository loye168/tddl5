package com.taobao.tddl.client.sequence.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.ali.unit.rule.RouterTestHelper;
import com.taobao.tddl.client.sequence.exception.SequenceException;

/**
 * @description
 * @author <a href="junyu@taobao.com">junyu</a>
 */
@Ignore("手工跑,需要单独依赖tddl-client包")
public class UnitGroupSequenceDaoUnitTest {

    private static Set<String> testUnits;
    private static String      currentUnit;

    @BeforeClass
    public static void setUp() {
        testUnits = new HashSet<String>();
        testUnits.add("UNIT_CM3");
        testUnits.add("UNIT_XINGYI");
        testUnits.add("UNIT_DONGGUAN");
        testUnits.add("UNIT_CM9");
        currentUnit = "UNIT_CM9";
    }

    @Test
    public void testGetRealDbGroupKeys() {
        UnitGroupSequenceDao dao = new UnitGroupSequenceDao();
        ArrayList<String> dbGroupKeys = new ArrayList<String>();
        dbGroupKeys.add("GROUP1");
        dbGroupKeys.add("GROUP2");
        dbGroupKeys.add("GROUP3");
        dao.setDbGroupKeys(dbGroupKeys);
        dao.setAppName("TEST_APP");
        dao.setDscount(3);
        dao.setInnerStep(1000);
        RouterTestHelper.setTestData(testUnits, currentUnit);
        try {
            List<String> groups = dao.getRealDbGroupKeys();
            // assert
            Assert.assertEquals(12, groups.size());
            Assert.assertEquals(UnitGroupSequenceDao.DUMMY_OFF_DBGROUPKEY, groups.get(0));
            Assert.assertEquals(UnitGroupSequenceDao.DUMMY_OFF_DBGROUPKEY, groups.get(1));
            Assert.assertEquals(UnitGroupSequenceDao.DUMMY_OFF_DBGROUPKEY, groups.get(2));
            Assert.assertEquals("GROUP1", groups.get(3));
            Assert.assertEquals("GROUP2", groups.get(4));
            Assert.assertEquals("GROUP3", groups.get(5));
            Assert.assertEquals(UnitGroupSequenceDao.DUMMY_OFF_DBGROUPKEY, groups.get(6));
            Assert.assertEquals(UnitGroupSequenceDao.DUMMY_OFF_DBGROUPKEY, groups.get(7));
            Assert.assertEquals(UnitGroupSequenceDao.DUMMY_OFF_DBGROUPKEY, groups.get(8));
            Assert.assertEquals(UnitGroupSequenceDao.DUMMY_OFF_DBGROUPKEY, groups.get(9));
            Assert.assertEquals(UnitGroupSequenceDao.DUMMY_OFF_DBGROUPKEY, groups.get(10));
            Assert.assertEquals(UnitGroupSequenceDao.DUMMY_OFF_DBGROUPKEY, groups.get(11));
        } catch (SequenceException e) {
            Assert.fail();
        }
    }

    @Test
    public void testDynamicChange() {
        UnitGroupSequenceDao dao = new UnitGroupSequenceDao();
        ArrayList<String> dbGroupKeys = new ArrayList<String>();
        dbGroupKeys.add("GROUP1");
        dbGroupKeys.add("GROUP2");
        dbGroupKeys.add("GROUP3");
        dao.setDbGroupKeys(dbGroupKeys);
        dao.setAppName("TEST_APP");
        dao.setDscount(3);
        dao.setInnerStep(1000);
        RouterTestHelper.setTestData(testUnits, currentUnit);
        RouterTestHelper.setTestData(testUnits, "UNIT_DONGGUAN");
        try {
            List<String> groups = dao.getRealDbGroupKeys();
            // assert
            Assert.assertEquals(12, groups.size());
            Assert.assertEquals(UnitGroupSequenceDao.DUMMY_OFF_DBGROUPKEY, groups.get(0));
            Assert.assertEquals(UnitGroupSequenceDao.DUMMY_OFF_DBGROUPKEY, groups.get(1));
            Assert.assertEquals(UnitGroupSequenceDao.DUMMY_OFF_DBGROUPKEY, groups.get(2));
            Assert.assertEquals(UnitGroupSequenceDao.DUMMY_OFF_DBGROUPKEY, groups.get(3));
            Assert.assertEquals(UnitGroupSequenceDao.DUMMY_OFF_DBGROUPKEY, groups.get(4));
            Assert.assertEquals(UnitGroupSequenceDao.DUMMY_OFF_DBGROUPKEY, groups.get(5));
            Assert.assertEquals("GROUP1", groups.get(6));
            Assert.assertEquals("GROUP2", groups.get(7));
            Assert.assertEquals("GROUP3", groups.get(8));
            Assert.assertEquals(UnitGroupSequenceDao.DUMMY_OFF_DBGROUPKEY, groups.get(9));
            Assert.assertEquals(UnitGroupSequenceDao.DUMMY_OFF_DBGROUPKEY, groups.get(10));
            Assert.assertEquals(UnitGroupSequenceDao.DUMMY_OFF_DBGROUPKEY, groups.get(11));
        } catch (SequenceException e) {
            Assert.fail();
        }
        Set<String> testUnits2 = new HashSet<String>();
        testUnits2.add("UNIT_CM3");
        testUnits2.add("UNIT_XINGYI");
        testUnits2.add("UNIT_DONGGUAN");
        testUnits2.add("UNIT_CM9");
        testUnits2.add("UNIT_CM6");
        RouterTestHelper.setTestData(testUnits2, "UNIT_DONGGUAN");
        try {
            List<String> groups = dao.getRealDbGroupKeys();
            // assert
            Assert.assertEquals(15, groups.size());
            Assert.assertEquals(UnitGroupSequenceDao.DUMMY_OFF_DBGROUPKEY, groups.get(0));
            Assert.assertEquals(UnitGroupSequenceDao.DUMMY_OFF_DBGROUPKEY, groups.get(1));
            Assert.assertEquals(UnitGroupSequenceDao.DUMMY_OFF_DBGROUPKEY, groups.get(2));
            Assert.assertEquals(UnitGroupSequenceDao.DUMMY_OFF_DBGROUPKEY, groups.get(3));
            Assert.assertEquals(UnitGroupSequenceDao.DUMMY_OFF_DBGROUPKEY, groups.get(4));
            Assert.assertEquals(UnitGroupSequenceDao.DUMMY_OFF_DBGROUPKEY, groups.get(5));
            Assert.assertEquals(UnitGroupSequenceDao.DUMMY_OFF_DBGROUPKEY, groups.get(6));
            Assert.assertEquals(UnitGroupSequenceDao.DUMMY_OFF_DBGROUPKEY, groups.get(7));
            Assert.assertEquals(UnitGroupSequenceDao.DUMMY_OFF_DBGROUPKEY, groups.get(8));
            Assert.assertEquals("GROUP1", groups.get(9));
            Assert.assertEquals("GROUP2", groups.get(10));
            Assert.assertEquals("GROUP3", groups.get(11));
            Assert.assertEquals(UnitGroupSequenceDao.DUMMY_OFF_DBGROUPKEY, groups.get(12));
            Assert.assertEquals(UnitGroupSequenceDao.DUMMY_OFF_DBGROUPKEY, groups.get(13));
            Assert.assertEquals(UnitGroupSequenceDao.DUMMY_OFF_DBGROUPKEY, groups.get(14));
        } catch (SequenceException e) {
            Assert.fail();
        }
    }
}
