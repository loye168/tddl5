package com.alibaba.cobar.manager.qa.sysadmin;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.alibaba.cobar.manager.dao.xml.UserDAOImple;
import com.alibaba.cobar.manager.dao.xml.XMLFileLoaderPath;
import com.alibaba.cobar.manager.dataobject.xml.UserDO;
import com.alibaba.cobar.manager.util.ConstantDefine;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * @author xiaowen.guoxw
 * @version ???????2011-6-27 ????03:19:57
 */

public class UserDaoTest extends SysAdminTest {

    private static final Logger logger        = LoggerFactory.getLogger(UserDaoTest.class);
    private UserDAOImple        userDao       = null;
    private String              userXmlPath   = null;
    private XMLFileLoaderPath   xmlFileLoader = null;

    @Before
    public void initData() {
        Assert.assertNotNull(xmlPath);
        userDao = new UserDAOImple();
        xmlFileLoader = new XMLFileLoaderPath();
        xmlFileLoader.setXmlPath(xmlPath);
        userDao.setXmlFileLoader(xmlFileLoader);
        userXmlPath = xmlPath + System.getProperty("file.separator") + "user.xml";
        XmlFile xmlFile = new XmlFile(userXmlPath, "users");
        try {
            xmlFile.init();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            Assert.fail();
        }
        read();
    }

    @Test
    public void testAddFirstUser() {
        Assert.assertEquals(0, userDao.getUserList().size());
        UserDO user = DOFactory.getUser();
        userDao.addUser(user);

        Assert.assertEquals(1, userDao.getUserList().size());
        read();
        Assert.assertEquals(1, userDao.getUserList().size());
    }

    @Test
    public void testAddManyUsers() {
        int userNum = 10;
        for (int i = 0; i < userNum; i++) {
            UserDO user = DOFactory.getUser();
            user.setUsername("test" + i);
            userDao.addUser(user);
        }

        Assert.assertEquals(userNum, userDao.getUserList().size());
        read();
        Assert.assertEquals(userNum, userDao.getUserList().size());
    }

    @Test
    public void testAddDuplicatedUsers() {
        int userNum = 10;
        UserDO user = DOFactory.getUser();
        user.setRealname("test");
        Assert.assertTrue(userDao.addUser(user));
        Assert.assertEquals(1, userDao.getUserList().size());

        for (int i = 0; i < userNum; i++) {
            UserDO userDuplicated = DOFactory.getUser();
            userDuplicated.setRealname("test" + i);
            Assert.assertFalse(userDao.addUser(userDuplicated));
        }

        Assert.assertEquals(1, userDao.getUserList().size());
        read();
        Assert.assertEquals(1, userDao.getUserList().size());
    }

    @Test
    public void testModifyUsers() {
        UserDO user = DOFactory.getUser();
        userDao.addUser(user);
        UserDO destUser = DOFactory.getUser();
        long destId = user.getId();
        String destPassword = user.getPassword() + "test";
        String destRealName = user.getRealname() + "test";
        String destStatus = user.getStatus();
        String destUserRole = ConstantDefine.ACTIVE.equals(user.getStatus()) ? ConstantDefine.IN_ACTIVE : ConstantDefine.ACTIVE;
        String destUserName = user.getUsername() + "test";

        destUser.setId(user.getId());
        destUser.setPassword(destPassword);
        destUser.setRealname(destRealName);
        destUser.setStatus(destStatus);
        destUser.setUser_role(destUserRole);
        destUser.setUsername(destUserName);
        Assert.assertTrue(userDao.modifyUser(destUser));

        Assert.assertEquals(1, userDao.getUserList().size());
        Assert.assertEquals(destPassword, userDao.getUserById(destId).getPassword());
        Assert.assertEquals(destRealName, userDao.getUserById(destId).getRealname());
        Assert.assertEquals(destStatus, userDao.getUserById(destId).getStatus());
        Assert.assertEquals(destUserRole, userDao.getUserById(destId).getUser_role());
        Assert.assertEquals(destUserName, userDao.getUserById(destId).getUsername());
    }

    @Test
    public void testValidateUsers() {
        UserDO user = DOFactory.getUser();
        String userName = user.getUsername();
        String password = user.getPassword();
        userDao.addUser(user);

        UserDO userValidate = userDao.validateUser(userName, password);
        Assert.assertNotNull(userValidate);
        Assert.assertSame(user, userValidate);
        Assert.assertNull(userDao.validateUser(userName + "test", password));
        Assert.assertNull(userDao.validateUser(userName, password + "test"));

    }

    public void read() {
        try {
            xmlFileLoader.setXmlPath(xmlPath);
            userDao.afterPropertiesSet();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            Assert.fail();
        }
    }
}
