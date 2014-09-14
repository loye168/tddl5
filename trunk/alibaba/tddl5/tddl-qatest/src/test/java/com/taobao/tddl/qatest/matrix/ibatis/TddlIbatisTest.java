package com.taobao.tddl.qatest.matrix.ibatis;

import java.sql.SQLException;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import com.ibatis.sqlmap.client.SqlMapClient;
import com.taobao.tddl.config.ConfigDataMode;
import com.taobao.tddl.config.ConfigDataMode.Mode;
import com.taobao.tddl.qatest.util.TddlTestDO;

/**
 * ibatis 测试
 * 
 * @author zhuoxue
 * @since 5.0.1
 */
public class TddlIbatisTest {

    private static ClassPathXmlApplicationContext context             = null;
    private static SqlMapClient                   sqlMapClient        = null;
    private static TransactionTemplate            transactionTempalte = null;

    @BeforeClass
    public static void setUp() {
        Mode mode = ConfigDataMode.getMode();
        try {
            ConfigDataMode.setMode(Mode.AUTO);
            context = new ClassPathXmlApplicationContext("classpath:spring/spring_sample.xml");
            sqlMapClient = (SqlMapClient) context.getBean("sqlmap");
            transactionTempalte = (TransactionTemplate) context.getBean("transactionTemplate");
        } finally {
            ConfigDataMode.setMode(mode);

        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (context != null) {
            context.close();
            context = null;
            sqlMapClient = null;
            transactionTempalte = null;
        }
    }

    /**
     * 在tddl上 delete insert query
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void testDeleteInsertAndQuery() throws Exception {
        try {

            int result = sqlMapClient.delete("deleteUser", 1l);
            // ----无意义的判断哦！！，讲解JDBC用的---
            if (result > 0) {
                // 这表示这个数据已经存在
            } else {
                // 这表示这个数据不存在
            }
        } catch (Exception e) {
            throw e;
        }

        // ==============插入例子=====================
        try {
            TddlTestDO user = new TddlTestDO();
            user.setId(1);
            user.setName("ljh");

            int result = sqlMapClient.update("insertUser", user);
            System.out.println(user.getId());
            // ----无意义的判断哦！！，讲解JDBC用的---
            if (result > 0) {
                // 表示插入成功。
            } else {
                // 这个一般不会出现小于0的情况
            }
        } catch (Exception e) {
            throw e;
        }

        // =========查询的例子================

        try {

            TddlTestDO user = (TddlTestDO) sqlMapClient.queryForObject("queryUserById", 1l);
            Assert.assertEquals("ljh", user.getName());
        } catch (Exception e) {
            throw e;
        }
    }

    /**
     * 在一个表上进行 delete insert query
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void testDeleteInsertAndQuery_oneDb() throws Exception {
        long id = 1l;
        try {
            int result = sqlMapClient.delete("deleteUser_one", id);
            // ----无意义的判断哦！！，讲解JDBC用的---
            if (result > 0) {
                // 这表示这个数据已经存在
            } else {
                // 这表示这个数据不存在
            }
        } catch (Exception e) {
            throw e;
        }

        // ==============插入例子=====================
        try {
            TddlTestDO user = new TddlTestDO();
            user.setName("ljh");

            sqlMapClient.insert("insertUser_one", user);
            id = user.getId();
        } catch (Exception e) {
            throw e;
        }

        // =========查询的例子================

        try {
            TddlTestDO user = (TddlTestDO) sqlMapClient.queryForObject("queryUserById_one", id);
            Assert.assertEquals("ljh", user.getName());

            int result = sqlMapClient.delete("deleteUser_one", id);
            Assert.assertEquals(1, result);
        } catch (Exception e) {
            throw e;
        }
    }

    /**
     * 这是一个事务的例子。 先进行一次插入，并维持连接，在连接上做一次查询确认插入是成功的。 然后做回滚取消这条记录。
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void testTransactions() throws Exception {
        final long userId = 2;
        final String userName = "ljh + " + userId;
        // 开启事务
        transactionTempalte.execute(new TransactionCallback() {

            @Override
            public Object doInTransaction(TransactionStatus status) {
                // 执行插入
                // ===========事务中插入一条记录================
                TddlTestDO user = new TddlTestDO();
                user.setId(userId);
                user.setName(userName);

                try {
                    int result = sqlMapClient.update("insertUser", user);
                    Assert.assertEquals(1, result);
                    // 执行查询
                    // ============在事务中查询刚才插入的数据==================
                    TddlTestDO userFromDB = (TddlTestDO) sqlMapClient.queryForObject("queryUserById", userId);
                    Assert.assertEquals(user.getName(), userFromDB.getName());
                    // 回滚事务
                    status.setRollbackOnly();
                    return null;
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }
}
