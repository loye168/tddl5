package com.alibaba.cobar.manager.qa.monitor;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.alibaba.cobar.manager.dataobject.cobarnode.CommandStatus;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

public class TestCommands extends TestCobarAdapter {

    private static final Logger logger         = LoggerFactory.getLogger(TestCommands.class);
    private Connection          dmlConnection  = null;
    private List<CommandStatus> commandList    = null;
    protected long              initDBNum      = 0;
    protected long              queryNum       = 0;
    protected long              stmtPrepareNum = 0;
    protected long              stmtExcuteNum  = 0;
    protected long              stmtClosed     = 0;
    protected long              pingNum        = 0;
    protected long              killNum        = 0;
    protected long              quitNum        = 0;
    protected long              otherNum       = 0;

    @Override
    @Before
    public void initData() {
        super.initData();
        try {
            if (null != dmlConnection) {
                dmlConnection.close();
            }
            dmlConnection = sCobarNode.createDMLConnection("ddl_test");
            if (null != commandList) {
                commandList.clear();
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            Assert.fail();
        }
    }

    @Override
    @After
    public void end() {
        super.end();
        try {
            if (null != dmlConnection) {
                dmlConnection.close();
            }
            if (null != commandList) {
                commandList.clear();
            }
        } catch (Exception e) {
            logger.error("destroy adpter error");
            Assert.fail();
        }
    }

    /*
     * statistic sum count of each column
     */
    public void statistic() {
        initDBNum = 0;
        queryNum = 0;
        stmtPrepareNum = 0;
        stmtExcuteNum = 0;
        stmtClosed = 0;
        pingNum = 0;
        killNum = 0;
        quitNum = 0;
        otherNum = 0;

        commandList = cobarAdapter.listCommandStatus();
        for (CommandStatus command : commandList) {
            initDBNum += command.getInitDB();
            queryNum += command.getQuery();
            stmtPrepareNum += command.getStmtPrepared();
            stmtExcuteNum += command.getStmtExecute();
            stmtClosed += command.getStmtClose();
            pingNum += command.getPing();
            killNum += command.getKill();
            quitNum += command.getQuit();
            otherNum += command.getOther();
        }

    }

    /*
     * Execute "show tables" in dmlConnection, sum of query will increase 1
     */
    @Test
    public void testQurey() {
        statistic();
        long queryNumBefore = queryNum;
        try {
            sCobarNode.executeSQLRead(dmlConnection, "show tables");
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
            Assert.fail();
        }
        statistic();
        Assert.assertEquals(queryNum, queryNumBefore + 1);
    }

    @Ignore
    @Test
    public void testStmtExcute() {
        try {
            statistic();
            long stmtExcuteNumBefore = this.stmtExcuteNum;
            sCobarNode.excuteSQWrite(dmlConnection, "create table test1 (name varchar (20))");
            statistic();
            Assert.assertEquals(stmtExcuteNum, stmtExcuteNumBefore + 1);
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
            Assert.fail();
        } finally {
            try {
                sCobarNode.excuteSQWrite(dmlConnection, "drop table test1");
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
                Assert.fail();
            }
        }
    }

    @Ignore
    @Test
    public void testInitDB() {
        try {
            statistic();
            long initNumBefore = this.initDBNum;
            sCobarNode.excuteSQWrite(dmlConnection, "create database test1");
            statistic();
            Assert.assertEquals(initDBNum, initNumBefore + 1);
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
            Assert.fail();
        } finally {
            try {
                sCobarNode.excuteSQWrite(dmlConnection, "drop database test1");
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
                Assert.fail();
            }
        }
    }

}
