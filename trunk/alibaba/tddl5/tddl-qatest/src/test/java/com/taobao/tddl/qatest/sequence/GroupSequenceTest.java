package com.taobao.tddl.qatest.sequence;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.dao.DataAccessException;

import com.taobao.diamond.mockserver.MockServer;
import com.taobao.tddl.client.sequence.exception.SequenceException;
import com.taobao.tddl.client.sequence.impl.GroupSequence;
import com.taobao.tddl.client.sequence.impl.GroupSequenceDao;
import com.taobao.tddl.common.GroupDataSourceRouteHelper;
import com.taobao.tddl.qatest.BaseAtomGroupTestCase;

/**
 * group sequence
 * 
 * @author zhuoxue
 * @since 5.0.1
 */
public class GroupSequenceTest extends BaseAtomGroupTestCase {

    protected static ClassPathXmlApplicationContext context = null;
    protected static GroupSequence                  seque   = null;
    private Set<Long>                               set     = new HashSet<Long>();
    private AtomicInteger                           seqCnt  = new AtomicInteger();

    @BeforeClass
    public static void setUp() throws Exception {
        MockServer.setUpMockServer();
        setMatrixMockInfo(BaseAtomGroupTestCase.MATRIX_DBGROUPS_PATH, BaseAtomGroupTestCase.TDDL_DBGROUPS);
    }

    @Before
    public void before() {
        context = new ClassPathXmlApplicationContext(new String[] { "classpath:sequence/spring_context_group_sequence.xml" });
        seque = (GroupSequence) context.getBean("sequence");
    }

    @After
    public void after() {
        context.destroy();
    }

    /**
     * nextValue()函数
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void getNextValueTest() throws SequenceException {
        long value = seque.nextValue();
        long nextValue = seque.nextValue();
        Assert.assertEquals(value + 1, nextValue);
    }

    /**
     * 修改sequence的初始值
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void nextValueTest() throws Exception {
        Connection con = getConnection("qatest_normal_0");
        Statement stmt = (Statement) con.createStatement();
        stmt.executeUpdate("update sequence set value='0' where name='ni'");
        con = getConnection("qatest_normal_1");
        stmt = (Statement) con.createStatement();
        stmt.executeUpdate("update sequence set value='100' where name='ni'");
        stmt.close();
        con.close();
        long value = seque.nextValue();
        Assert.assertTrue(value == 301 || value == 201);
    }

    /**
     * 同一个线程 不同的实例nextValue()返回值是否递增
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void sameTreadTest() throws SequenceException {
        long value = seque.nextValue();
        GroupSequence seque1 = (GroupSequence) context.getBean("sequence");
        long value1 = seque1.nextValue();
        Assert.assertEquals(value + 1, value1);
    }

    /**
     * 不同的线程
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void difTreadTest() throws Exception {
        Connection con = getConnection("qatest_normal_0");
        Statement stmt = (Statement) con.createStatement();
        stmt.executeUpdate("update sequence set value='0' where name='ni'");
        con = getConnection("qatest_normal_1");
        stmt = (Statement) con.createStatement();
        stmt.executeUpdate("update sequence set value='100' where name='ni'");
        stmt.close();
        con.close();
        long value = seque.nextValue();
        Assert.assertTrue(value == 301 || value == 201);
        GroupSequenceDao sequeDao = (GroupSequenceDao) context.getBean("sequenceDao");
        GroupSequence seque = new GroupSequence();
        seque.setName("ni");
        seque.setSequenceDao(sequeDao);
        long value1 = seque.nextValue();
        Assert.assertTrue(value1 == 201 || value1 == 301 || value1 == 401 || value1 == 501);
    }

    /**
     * 大步长
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void greaterStepTest() throws Exception {
        Connection con = getConnection("qatest_normal_0");
        Statement stmt = (Statement) con.createStatement();
        stmt.executeUpdate("update sequence set value='0' where name='ni'");
        con = getConnection("qatest_normal_1");
        stmt = (Statement) con.createStatement();
        stmt.executeUpdate("update sequence set value='100' where name='ni'");
        stmt.close();
        con.close();

        long value = 0l;
        for (int i = 0; i < 150; i++) {
            value = seque.nextValue();
        }
        Assert.assertTrue(value == 250 || value == 350 || value == 450 || value == 550);
    }

    /**
     * 小步长
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void lessStepTest() throws Exception {
        Connection con = getConnection("qatest_normal_0");
        Statement stmt = (Statement) con.createStatement();
        stmt.executeUpdate("update sequence set value='0' where name='ni'");
        con = getConnection("qatest_normal_1");
        stmt = (Statement) con.createStatement();
        stmt.executeUpdate("update sequence set value='100' where name='ni'");
        stmt.close();
        con.close();

        long value = 0l;
        for (int i = 0; i < 50; i++) {
            value = seque.nextValue();
        }
        Assert.assertTrue(value == 250 || value == 350);

    }

    /**
     * 初始值为0 小步长
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void startWith0GetTwoValueLessStep() throws Exception {
        Connection con = getConnection("qatest_normal_0");
        Statement stmt = (Statement) con.createStatement();
        stmt.executeUpdate("update sequence set value='0' where name='ni'");
        con = getConnection("qatest_normal_1");
        stmt = (Statement) con.createStatement();
        stmt.executeUpdate("update sequence set value='0' where name='ni'");
        stmt.close();
        con.close();

        Long value = seque.nextValue();
        System.out.println(value);
        GroupSequenceDao sequeDao = (GroupSequenceDao) context.getBean("sequenceDao");
        GroupSequence seque = new GroupSequence();
        seque.setName("ni");
        seque.setSequenceDao(sequeDao);

        value = seque.nextValue();
        int key1 = 0;
        int key2 = 0;
        con = getConnection("qatest_normal_0");
        stmt = (Statement) con.createStatement();
        ResultSet rs = stmt.executeQuery("select * from sequence where name='ni'");
        while (rs.next()) {
            key1 = rs.getInt(2);
        }
        con = getConnection("qatest_normal_1");
        stmt = (Statement) con.createStatement();
        rs = stmt.executeQuery("select * from sequence where name='ni'");
        while (rs.next()) {
            key2 = rs.getInt(2);
        }

        int a = (key1 / 100) % 2;
        int b = (key2 / 100) % 2;
        Assert.assertFalse(a == b);
    }

    /**
     * 初始值为0 大步长
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void statrWith0GreaterStep() throws Exception {
        Connection con = getConnection("qatest_normal_0");
        Statement stmt = (Statement) con.createStatement();
        stmt.executeUpdate("update sequence set value='0' where name='ni'");
        con = getConnection("qatest_normal_1");
        stmt = (Statement) con.createStatement();
        stmt.executeUpdate("update sequence set value='100' where name='ni'");
        stmt.close();
        con.close();

        Long value = 0l;
        for (int i = 0; i < 150; i++) {
            value = seque.nextValue();
        }
        value = seque.nextValue();
        System.out.println(value);
        GroupSequenceDao sequeDao = (GroupSequenceDao) context.getBean("sequenceDao");
        GroupSequence seque = new GroupSequence();
        seque.setName("ni");
        seque.setSequenceDao(sequeDao);

        for (int i = 0; i < 150; i++) {// 多取几次，保证一定能取到
            value = seque.nextValue();
        }
        int key1 = 0;
        int key2 = 0;
        con = getConnection("qatest_normal_0");
        stmt = (Statement) con.createStatement();
        ResultSet rs = stmt.executeQuery("select * from sequence where name='ni'");
        while (rs.next()) {
            key1 = rs.getInt(2);
        }
        con = getConnection("qatest_normal_1");
        stmt = (Statement) con.createStatement();
        rs = stmt.executeQuery("select * from sequence where name='ni'");
        while (rs.next()) {
            key2 = rs.getInt(2);
        }

        int a = (key1 / 100) % 2;
        int b = (key2 / 100) % 2;
        Assert.assertFalse(a == b);
    }

    /**
     * 多实例
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void multiTest() throws SequenceException {
        int times = 100;
        for (int i = 0; i < times; i++) {
            GroupSequenceDao sequeDao = (GroupSequenceDao) context.getBean("sequenceDao");
            GroupSequence sq = new GroupSequence();
            sq.setName("ni");
            sq.setSequenceDao(sequeDao);
            set.add(sq.nextValue());
        }
        Assert.assertEquals(times, set.size());
    }

    /**
     * 多线程，两个db
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void multiThreadTwoDbTest() throws SequenceException, InterruptedException {
        ExecutorService es = Executors.newFixedThreadPool(100);
        final CountDownLatch count = new CountDownLatch(1);
        int times = 20;
        for (int i = 0; i < times; i++) {
            es.execute(new Runnable() {

                public void run() {
                    try {
                        count.await();
                    } catch (InterruptedException e) {
                    }
                    try {
                        GroupSequenceDao sequeDao = (GroupSequenceDao) context.getBean("sequenceDao");
                        GroupSequence sq = new GroupSequence();
                        sq.setName("ni");
                        sq.setSequenceDao(sequeDao);
                        set.add(sq.nextValue());
                        seqCnt.getAndIncrement();
                    } catch (DataAccessException e) {
                    } catch (SequenceException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        count.countDown();
        while (seqCnt.get() < times) {
            TimeUnit.MICROSECONDS.sleep(10);
        }
        Assert.assertEquals(times, set.size());
        es.shutdownNow();
    }

    /**
     * 多线程，一个db
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void multiThreadOneDbTest() throws SequenceException, InterruptedException {
        ExecutorService es = Executors.newFixedThreadPool(100);
        final CountDownLatch count = new CountDownLatch(1);
        int times = 15;
        for (int i = 0; i < times; i++) {
            es.execute(new Runnable() {

                public void run() {
                    try {
                        count.await();
                    } catch (InterruptedException e) {
                    }

                    try {
                        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(0);
                        GroupSequenceDao sequeDao = (GroupSequenceDao) context.getBean("sequenceDao_one_db");
                        GroupSequence sq = new GroupSequence();
                        sq.setName("ni");
                        sq.setSequenceDao(sequeDao);
                        set.add(sq.nextValue());
                        seqCnt.getAndIncrement();
                    } catch (DataAccessException e) {
                    } catch (SequenceException e) {
                        e.printStackTrace();
                    } finally {
                        count.countDown();
                    }
                }
            });
        }
        count.countDown();
        while (seqCnt.get() < times) {
            TimeUnit.MICROSECONDS.sleep(10);
        }
        Assert.assertEquals(times, set.size());
        es.shutdownNow();
    }

    public Connection getConnection(String db) {
        Connection conn = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            String url = "jdbc:mysql://10.232.31.154:3306/" + db;
            String user = "tddl";
            String passWord = "tddl";
            conn = (Connection) DriverManager.getConnection(url, user, passWord);
            if (conn != null) {
                System.out.println("conn is null!");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }

}
