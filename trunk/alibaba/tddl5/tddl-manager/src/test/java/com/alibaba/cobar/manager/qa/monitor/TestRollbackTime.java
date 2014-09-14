package com.alibaba.cobar.manager.qa.monitor;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.alibaba.cobar.manager.qa.util.TestUtils;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

@RunWith(Parameterized.class)
public class TestRollbackTime extends TestCobarAdapter {

    private static final Logger logger            = LoggerFactory.getLogger(TestRollbackTime.class);
    private Connection          managerConnection = null;
    private String              rollbackSql       = null;

    @Override
    @Before
    public void initData() {
        super.initData();
        try {
            if (null != managerConnection) {
                managerConnection.close();
            }
            managerConnection = sCobarNode.createManagerConnection();
        } catch (Exception e) {
            logger.error("destroy adpter error");
            Assert.fail();
        }
    }

    @Override
    @After
    public void end() {
        super.end();
        try {
            if (null != managerConnection) {
                managerConnection.close();
            }
        } catch (Exception e) {
            logger.error("destroy adpter error");
            Assert.fail();
        }
    }

    @SuppressWarnings("rawtypes")
    @Parameters
    public static Collection prepareData() {
        Object[][] objs = { { "rollback @@config" } };
        return Arrays.asList(objs);
    }

    public TestRollbackTime(String rollbackSql){
        this.rollbackSql = rollbackSql;
    }

    /*
     * first, reload config, then rollback config && get first rollback time
     * reload config for next rollback ,sleep, get second rollback time, compare
     * two rollback time. note : if reloadtime >= rollbacktime then rollback
     * failure!!!
     */
    @Test
    public void testRollbackTime() {
        try {
            long sleepTime = 1000;
            sCobarNode.excuteSQL(managerConnection, "reload @@config");
            sCobarNode.excuteSQL(managerConnection, rollbackSql);
            long initRollbackTime = cobarAdapter.getServerStatus().getRollbackTime();

            TestUtils.waitForMonment(sleepTime);// must sleep, otherwise second
                                                // rollback will failure

            sCobarNode.excuteSQL(managerConnection, "reload @@config");
            TestUtils.waitForMonment(sleepTime);
            sCobarNode.excuteSQL(managerConnection, rollbackSql);
            long rollbackTime = cobarAdapter.getServerStatus().getRollbackTime();

            Assert.assertFalse(rollbackTime == initRollbackTime);
            Assert.assertTrue(rollbackTime - initRollbackTime >= sleepTime);
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
            Assert.fail();
        }
    }

}
