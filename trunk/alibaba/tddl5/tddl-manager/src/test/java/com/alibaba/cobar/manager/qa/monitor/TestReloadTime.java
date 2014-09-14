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
public class TestReloadTime extends TestCobarAdapter {

    private static final Logger logger            = LoggerFactory.getLogger(TestReloadTime.class);
    public Connection           managerConnection = null;
    private String              reloadSql         = null;

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
    public static Collection perpareData() {
        Object[][] objs = { { "reload @@config" }, { "reload @@user" } };
        return Arrays.asList(objs);
    }

    public TestReloadTime(String reloadSql){
        this.reloadSql = reloadSql;
    }

    @Test
    public void testReloadTime() {
        try {
            // show @@server
            long timeBeforeReload = cobarAdapter.getServerStatus().getReloadTime();
            long sleepTime = 1000;
            TestUtils.waitForMonment(sleepTime);

            // reload @@config
            Assert.assertTrue(sCobarNode.excuteSQL(managerConnection, reloadSql));
            long timeAfterReload = cobarAdapter.getServerStatus().getReloadTime();
            Assert.assertFalse(timeBeforeReload == timeAfterReload);
            Assert.assertTrue(timeAfterReload - timeBeforeReload >= sleepTime);
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
            Assert.fail();
        }
    }
}
