package com.alibaba.cobar.manager.qa.monitor;

import java.sql.Connection;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.alibaba.cobar.manager.qa.modle.CobarFactory;
import com.alibaba.cobar.manager.qa.modle.SimpleCobarNode;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

public class TestCobarNode {

    private static final Logger logger        = LoggerFactory.getLogger(TestCobarNode.class);
    private SimpleCobarNode     cobarNode;
    private Connection          dmlConnection = null;

    @BeforeClass
    public static void init() {

    }

    @Before
    public void initConnection() {
        try {
            cobarNode = CobarFactory.getSimpleCobarNode("cobar");
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            Assert.fail();
        }
    }

    public void getDMLConnection(String schema) {
        if (null != dmlConnection) {
            try {
                dmlConnection.close();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                Assert.fail();
            }
        }
        try {
            dmlConnection = cobarNode.createDMLConnection(schema);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            Assert.fail();
        }
    }

    @Ignore
    @Test
    public void testAddConnections() {
        Connection conn = null;
        try {
            try {
                conn = cobarNode.createManagerConnection();
            } finally {
                Assert.assertTrue(cobarNode.detoryConnection(conn));
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            Assert.fail();
        }
    }

    @Ignore
    @Test
    public void testExcuteReadSql() {
        Connection conn = null;
        try {
            try {
                conn = cobarNode.createDMLConnection("ddl_test");
                cobarNode.executeSQLRead(conn, "select * from animals");
            } finally {
                Assert.assertTrue(cobarNode.detoryConnection(conn));
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            Assert.fail();
        }
    }

    @Ignore
    @Test
    public void testExcuteWriteSql() {
        getDMLConnection("ddl_test");
        try {
            try {
                cobarNode.excuteSQWrite(dmlConnection, "insert into animals (name) values('name')");
            } finally {
                Assert.assertTrue(cobarNode.detoryConnection(dmlConnection));
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            Assert.fail();
        }
    }

}
