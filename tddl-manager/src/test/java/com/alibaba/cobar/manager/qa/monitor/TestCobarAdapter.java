package com.alibaba.cobar.manager.qa.monitor;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;

import com.alibaba.cobar.manager.dao.delegate.CobarAdapter;
import com.alibaba.cobar.manager.qa.modle.CobarFactory;
import com.alibaba.cobar.manager.qa.modle.SimpleCobarNode;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

public class TestCobarAdapter {

    public static CobarAdapter    cobarAdapter = null;
    public static SimpleCobarNode sCobarNode   = null;

    private static final Logger   logger       = LoggerFactory.getLogger(TestCobarAdapter.class);

    @BeforeClass
    public static void init() {
        try {
            cobarAdapter = CobarFactory.getCobarAdapter("cobar");
            sCobarNode = CobarFactory.getSimpleCobarNode("cobar");
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            Assert.fail();
        }
    }

    @Before
    public void initData() {
        if (null != cobarAdapter && null != cobarAdapter.getDataSource()) {
            try {
                cobarAdapter.destroy();
            } catch (Exception e) {
                logger.error("destroy adpter error");
                Assert.fail();
            }
        }
    }

    @After
    public void end() {
        try {
            if (null != cobarAdapter && null != cobarAdapter.getDataSource()) {
                cobarAdapter.destroy();
            }

        } catch (Exception e) {
            logger.error("destroy adpter error");
            Assert.fail();
        }

    }

}
