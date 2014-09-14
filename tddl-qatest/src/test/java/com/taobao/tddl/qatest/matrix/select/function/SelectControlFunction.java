package com.taobao.tddl.qatest.matrix.select.function;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

import com.taobao.tddl.qatest.BaseMatrixTestCase;
import com.taobao.tddl.qatest.BaseTestCase;
import com.taobao.tddl.qatest.ExecuteTableName;
import com.taobao.tddl.qatest.util.EclipseParameterized;

/**
 * 控制函数
 * 
 * @author zhuoxue
 * @since 5.0.1
 */
@RunWith(EclipseParameterized.class)
public class SelectControlFunction extends BaseMatrixTestCase {

    @Parameters(name = "{index}:table={0}")
    public static List<String[]> prepareData() {
        return Arrays.asList(ExecuteTableName.normaltblTable(dbType));
    }

    public SelectControlFunction(String tableName){
        BaseTestCase.normaltblTableName = tableName;
    }

    @Before
    public void prepare() throws Exception {
        normaltblPrepare(0, 20);
    }

    @After
    public void destory() throws Exception {
        psConRcRsClose(rc, rs);
    }

    /**
     * if()
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void ifTest() throws Exception {
        if (normaltblTableName.startsWith("ob")) {
            return;
        }
        String sql = String.format("select if(pk<=id,id,pk) as m from %s", normaltblTableName);
        String[] columnParam = { "m" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = String.format("select sum(if(pk=id,id,pk)) as m from %s", normaltblTableName);
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

}
