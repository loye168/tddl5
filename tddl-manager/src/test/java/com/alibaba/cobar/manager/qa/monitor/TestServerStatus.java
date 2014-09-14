package com.alibaba.cobar.manager.qa.monitor;

import org.junit.Assert;
import org.junit.Test;

public class TestServerStatus extends TestCobarAdapter {

    @Test
    public void testStatus() {
        String status = cobarAdapter.getServerStatus().getStatus();
        Assert.assertEquals(status, "RUNNING");
    }

}
