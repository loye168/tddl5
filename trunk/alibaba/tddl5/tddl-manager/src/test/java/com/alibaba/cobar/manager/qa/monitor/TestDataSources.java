package com.alibaba.cobar.manager.qa.monitor;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.alibaba.cobar.manager.dataobject.cobarnode.DataNodesStatus;

public class TestDataSources extends TestCobarAdapter {

    @Test
    public void testDataSourcesCommand() {
        List<DataNodesStatus> dsStatusList = cobarAdapter.listDataNodes();
        Assert.assertTrue(dsStatusList.size() > 0);
    }
}
