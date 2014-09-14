package com.alibaba.cobar.manager.qa.monitor;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.alibaba.cobar.manager.dataobject.cobarnode.ProcessorStatus;
import com.alibaba.cobar.manager.qa.util.TestUtils;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

public class TestProcessor extends TestCobarAdapter {

    private static final Logger   logger     = LoggerFactory.getLogger(TestProcessor.class);
    private List<ProcessorStatus> psList     = null;
    private long                  netInSum   = 0;
    private long                  netOutSum  = 0;
    private long                  requestSum = 0;

    @Test(timeout = 60000)
    public void testListProcessorStatus() {
        TestUtils.waitForMonment(50000);
        int listNum = 10;
        List<Connection> connList = new ArrayList<Connection>();
        try {
            // create connection
            for (int i = 0; i < listNum; i++) {
                Connection conn = sCobarNode.createDMLConnection("ddl_test");
                connList.add(conn);
            }

            // get connection num from manager
            List<ProcessorStatus> psList = null;
            int connNum = 0;
            psList = cobarAdapter.listProccessorStatus();
            Assert.assertNotNull(psList);
            Assert.assertEquals(psList.size(), 8);
            for (ProcessorStatus ps : psList) {
                connNum += ps.getConnections();
            }
            Assert.assertEquals(connNum, listNum + 1);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            Assert.fail();
        } finally {
            for (Connection conn : connList) {
                Assert.assertTrue(sCobarNode.detoryConnection(conn));
            }
        }
    }

    public void statistic() {
        netInSum = 0;
        netOutSum = 0;
        requestSum = 0;
        psList = cobarAdapter.listProccessorStatus();
        for (ProcessorStatus ps : psList) {
            netInSum += ps.getNetIn();
            netOutSum += ps.getNetOut();
            requestSum += ps.getRequestCount();
        }
    }

    @Test(timeout = 60000)
    public void testNetInNetOut() {
        statistic();
        long netInSumBefore = netInSum;
        long netOutSumBefore = netOutSum;
        long requestSumBefore = requestSum;
        statistic();
        Assert.assertTrue(netInSum > netInSumBefore);
        Assert.assertTrue(netOutSum > netOutSumBefore);
        Assert.assertTrue(requestSum == (requestSumBefore + 1));
    }

}
