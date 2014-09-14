package com.alibaba.cobar.manager.qa.monitor;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public class TestDataBases extends TestCobarAdapter {

    @Test
    public void testDataBasesNum() {
        List<String> dataBasesList = cobarAdapter.listDataBases();
        Assert.assertTrue(dataBasesList.size() > 0);
    }
}
