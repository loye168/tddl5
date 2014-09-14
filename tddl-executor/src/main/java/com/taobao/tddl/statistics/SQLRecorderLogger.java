package com.taobao.tddl.statistics;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

public class SQLRecorderLogger {

    private final static Logger slowLogger              = LoggerFactory.getLogger("SLOW");
    private final static Logger mergeSlowLogger         = LoggerFactory.getLogger("MERGE_SLOW");

    private final static Logger pyhsicalSlowLogger      = LoggerFactory.getLogger("PYHSICAL_SLOW");
    private final static Logger mergePyhsicalSlowLogger = LoggerFactory.getLogger("MERGE_PYHSICAL_SLOW");

}
