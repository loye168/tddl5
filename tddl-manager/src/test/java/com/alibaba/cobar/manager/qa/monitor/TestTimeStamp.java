package com.alibaba.cobar.manager.qa.monitor;

import org.junit.Assert;
import org.junit.Test;

import com.alibaba.cobar.manager.qa.util.TestUtils;

public class TestTimeStamp extends TestCobarAdapter {

    @Test
    public void testGetVersion() {
        Assert.assertEquals("5.1.48-cobar-1.2.0", cobarAdapter.getVersion());
    }

    @Test
    public void testGetTimeCurrent() {
        com.alibaba.cobar.manager.dataobject.cobarnode.TimeStamp timeStamp = null;
        timeStamp = cobarAdapter.getCurrentTime();
        Assert.assertNotNull(timeStamp);
        long sleepTime = 1000L;
        long startTime = cobarAdapter.getCurrentTime().getTimestamp();
        TestUtils.waitForMonment(sleepTime);
        long endTime = cobarAdapter.getCurrentTime().getTimestamp();
        Assert.assertTrue(startTime + sleepTime <= endTime);
    }

    @Test
    public void testGetTimeStartUp() {
        String startTime = cobarAdapter.getServerStatus().getUptime();
        Assert.assertNotNull(startTime);
        long sleepTime = 1000L;
        TestUtils.waitForMonment(sleepTime);
        String endTime = cobarAdapter.getServerStatus().getUptime();
        Assert.assertNotNull(endTime);
    }

}
