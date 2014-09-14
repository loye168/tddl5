package com.taobao.tddl.qatest.matrix.template;

import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import com.taobao.tddl.qatest.BaseTemplateTestCase;

/**
 * JDBCTemplateTest
 * 
 * @author zhuoxue
 * @since 5.0.1
 */

public class JDBCTemplateTest extends BaseTemplateTestCase {

    /**
     * CRUD
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void CRUDTest() throws Exception {
        sql = String.format("insert into %s (pk,name) values(?,?)", normaltblTableName);
        andorJT.update(sql, new Object[] { RANDOM_ID, name });

        sql = String.format("select * from %s where pk= ?", normaltblTableName);
        Map re = andorJT.queryForMap(sql, new Object[] { RANDOM_ID });
        Assert.assertEquals(name, String.valueOf(re.get("name")));

        sql = String.format("update %s set name =? where pk=? ", normaltblTableName);
        andorJT.update(sql, new Object[] { name1, RANDOM_ID });

        sql = String.format("select * from %s where pk= ?", normaltblTableName);
        re = andorJT.queryForMap(sql, new Object[] { RANDOM_ID });
        Assert.assertEquals(name1, String.valueOf(re.get("name")));

        sql = String.format("delete from %s where pk = ?", normaltblTableName);
        andorJT.update(sql, new Object[] { RANDOM_ID });

        sql = String.format("select * from %s where pk= ?", normaltblTableName);
        List le = andorJT.queryForList(sql, new Object[] { RANDOM_ID });
        Assert.assertEquals(0, le.size());
    }

    /**
     * traction Commit
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void tractionCommitTest() {
        JdbcTemplate andorJT = new JdbcTemplate(tddlDatasource);
        DefaultTransactionDefinition def = new DefaultTransactionDefinition();
        DataSourceTransactionManager transactionManager = new DataSourceTransactionManager(tddlDatasource);
        TransactionStatus ts = transactionManager.getTransaction(def);

        try {
            sql = String.format("insert into %s (pk,name) values(?,?)", normaltblTableName);
            andorJT.update(sql, new Object[] { RANDOM_ID, name });
            sql = String.format("select * from %s where pk= ?", normaltblTableName);
            Map re = andorJT.queryForMap(sql, new Object[] { RANDOM_ID });
            Assert.assertEquals(name, String.valueOf(re.get("name")));
        } catch (DataAccessException ex) {
            transactionManager.rollback(ts);
            throw ex;
        } finally {
            transactionManager.commit(ts);
        }
        sql = String.format("select * from %s where pk= ?", normaltblTableName);
        Map re = andorJT.queryForMap(sql, new Object[] { RANDOM_ID });
        Assert.assertEquals(name, String.valueOf(re.get("name")));
    }

    /**
     * traction RollBack
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void tractionRollBackTest() {
        JdbcTemplate andorJT = new JdbcTemplate(tddlDatasource);
        DefaultTransactionDefinition def = new DefaultTransactionDefinition();
        DataSourceTransactionManager transactionManager = new DataSourceTransactionManager(tddlDatasource);
        TransactionStatus ts = transactionManager.getTransaction(def);

        try {
            sql = String.format("insert into %s (pk,name) values(?,?)", normaltblTableName);
            andorJT.update(sql, new Object[] { RANDOM_ID, name });
            sql = String.format("select * from %s where pk= ?", normaltblTableName);
            Map re = andorJT.queryForMap(sql, new Object[] { RANDOM_ID });
            Assert.assertEquals(name, String.valueOf(re.get("name")));
            // 回滚
            transactionManager.rollback(ts);
        } catch (DataAccessException ex) {
            transactionManager.rollback(ts);
            throw ex;
        } finally {
        }
        // 验证查询不到数据
        sql = String.format("select * from %s where pk= ?", normaltblTableName);
        List le = andorJT.queryForList(sql, new Object[] { RANDOM_ID });
        Assert.assertEquals(0, le.size());
    }
}
