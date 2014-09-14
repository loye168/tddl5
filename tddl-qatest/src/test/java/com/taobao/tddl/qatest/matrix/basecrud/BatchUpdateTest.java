package com.taobao.tddl.qatest.matrix.basecrud;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

import com.taobao.tddl.qatest.BaseMatrixTestCase;
import com.taobao.tddl.qatest.BaseTestCase;
import com.taobao.tddl.qatest.ExecuteTableName;
import com.taobao.tddl.qatest.util.EclipseParameterized;

/**
 * batch update 测试，mysql与tddl同时入库，入库结束后与mysql中内容相比较
 * 
 * @author zhuoxue
 * @since 5.0.1
 */
@RunWith(EclipseParameterized.class)
public class BatchUpdateTest extends BaseMatrixTestCase {

    @Parameters(name = "{index}:table={0}")
    public static List<String[]> prepareData() {
        return Arrays.asList(ExecuteTableName.normaltblTable(dbType));
    }

    public BatchUpdateTest(String tableName){
        BaseTestCase.normaltblTableName = tableName;
    }

    @Before
    public void initData() throws Exception {
        normaltblPrepare(0, 20);
    }

    /**
     * update all records
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void updateAll() throws Exception {

        if (normaltblTableName.startsWith("ob")) {
            // ob不支持批量更新
            return;
        }
        String sql = "UPDATE " + normaltblTableName
                     + " SET id=?,gmt_create=?,gmt_timestamp=?,gmt_datetime=?,name=?,floatCol=?";

        List<List<Object>> params = new ArrayList();

        for (int i = 0; i < 50; i++) {
            List<Object> param = new ArrayList<Object>();
            param.add(9999);
            param.add(gmtDay);
            param.add(gmt);
            param.add(gmt);
            param.add("new_name");
            param.add(0.999F);
            params.add(param);

        }
        executeBatch(sql, params);

        sql = "SELECT * FROM " + normaltblTableName;
        String[] columnParam = { "ID", "GMT_CREATE", "NAME", "FLOATCOL", "GMT_TIMESTAMP", "GMT_DATETIME" };
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * update one record
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void updateOne() throws Exception {

        if (normaltblTableName.startsWith("ob")) {
            // ob不支持批量更新
            return;
        }

        String sql = "UPDATE " + normaltblTableName
                     + " SET id=?,gmt_create=?,gmt_timestamp=?,gmt_datetime=?,name=?,floatCol=? WHERE pk=?";

        List<List<Object>> params = new ArrayList();

        for (int i = 0; i < 50; i++) {

            List<Object> param = new ArrayList<Object>();
            param.add(rand.nextInt());
            param.add(gmtDay);
            param.add(gmt);
            param.add(gmt);
            param.add("new_name" + rand.nextInt());
            param.add(fl);
            param.add(rand.nextInt(20));

            params.add(param);
        }

        executeBatch(sql, params);

        sql = "SELECT * FROM " + normaltblTableName;
        String[] columnParam = { "ID", "GMT_CREATE", "GMT_TIMESTAMP", "GMT_DATETIME", "NAME", "FLOATCOL" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * update some records with between
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void updateSome() throws Exception {
        if (normaltblTableName.startsWith("ob")) {
            // ob不支持批量更新
            return;
        }
        String sql = "UPDATE "
                     + normaltblTableName
                     + "  SET id=?,gmt_create=?,gmt_timestamp=?,gmt_datetime=?,name=?,floatCol=? WHERE pk BETWEEN ? AND ?";

        List<List<Object>> params = new ArrayList();

        for (int i = 0; i < 50; i++) {

            List<Object> param = new ArrayList<Object>();
            param.add(rand.nextInt());
            param.add(gmtDay);
            param.add(gmt);
            param.add(gmt);
            param.add("new_name" + rand.nextInt());
            param.add(fl);
            param.add(rand.nextInt(9));
            param.add(rand.nextInt(9) + 10);
            params.add(param);

        }

        executeBatch(sql, params);

        sql = "SELECT * FROM " + normaltblTableName + "  WHERE pk BETWEEN 3 AND 7";
        String[] columnParam = { "ID", "GMT_CREATE", "GMT_TIMESTAMP", "GMT_DATETIME", "NAME", "FLOATCOL" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * update some records with pk<? and pk>?
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void updateSome2() throws Exception {
        if (normaltblTableName.startsWith("ob")) {
            // ob不支持批量更新
            return;
        }
        String sql = "UPDATE " + normaltblTableName
                     + "  SET id=?,gmt_create=?,gmt_timestamp=?,gmt_datetime=?,name=?,floatCol=? WHERE pk<? and pk>?";

        List<List<Object>> params = new ArrayList();

        for (int i = 0; i < 50; i++) {

            List<Object> param = new ArrayList<Object>();
            param.add(rand.nextInt());
            param.add(gmtDay);
            param.add(gmt);
            param.add(gmt);
            param.add("new_name" + rand.nextInt());
            param.add(fl);
            param.add(rand.nextInt(9) + 10);
            param.add(rand.nextInt(9));

            params.add(param);

        }

        executeBatch(sql, params);

        sql = "SELECT * FROM " + normaltblTableName + "  WHERE pk BETWEEN 3 AND 7";
        String[] columnParam = { "ID", "GMT_CREATE", "GMT_TIMESTAMP", "GMT_DATETIME", "NAME", "FLOATCOL" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * update some records 参数位置发生变化
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void updateSome3() throws Exception {
        if (normaltblTableName.startsWith("ob")) {
            // ob不支持批量更新
            return;
        }
        String sql = "/*+TDDL({'extra':{'EXPAND_OR':'TRUE'}})*/ UPDATE "
                     + normaltblTableName
                     + "  SET id=?,gmt_create=?,gmt_timestamp=?,gmt_datetime=?,name=?,floatCol=? WHERE (pk>? or pk<?) and pk>?";

        List<List<Object>> params = new ArrayList();

        for (int i = 0; i < 50; i++) {

            List<Object> param = new ArrayList<Object>();
            param.add(rand.nextInt());
            param.add(gmtDay);
            param.add(gmt);
            param.add(gmt);
            param.add("new_name" + rand.nextInt());
            param.add(fl);
            param.add(rand.nextInt(9));
            param.add(rand.nextInt(9) + 10);
            param.add(rand.nextInt(9));

            params.add(param);

        }

        executeBatch(sql, params);

        sql = "SELECT * FROM " + normaltblTableName + " WHERE pk BETWEEN 3 AND 7";
        String[] columnParam = { "ID", "GMT_CREATE", "GMT_TIMESTAMP", "GMT_DATETIME", "NAME", "FLOATCOL" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * update some records 条件为 in()
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void updateSome4() throws Exception {
        if (normaltblTableName.startsWith("ob")) {
            // ob不支持批量更新
            return;
        }
        String sql = "UPDATE " + normaltblTableName
                     + "  SET id=?,gmt_create=?,gmt_timestamp=?,gmt_datetime=?,name=?,floatCol=? WHERE pk in (?,?)";

        List<List<Object>> params = new ArrayList();

        for (int i = 0; i < 50; i++) {

            List<Object> param = new ArrayList<Object>();
            param.add(rand.nextInt());
            param.add(gmtDay);
            param.add(gmt);
            param.add(gmt);
            param.add("new_name" + rand.nextInt());
            param.add(fl);
            param.add(rand.nextInt(5));
            param.add(rand.nextInt(5));

            params.add(param);

        }

        executeBatch(sql, params);

        sql = "SELECT * FROM " + normaltblTableName + "  WHERE pk BETWEEN 3 AND 7";
        String[] columnParam = { "ID", "GMT_CREATE", "GMT_TIMESTAMP", "GMT_DATETIME", "NAME", "FLOATCOL" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

}
