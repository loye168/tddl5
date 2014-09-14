package com.alibaba.cobar.manager.qa.sysadmin;

import java.io.File;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

public class SysAdminTest {

    private static final Logger logger  = LoggerFactory.getLogger(SysAdminTest.class);
    protected static String     xmlPath = null;
    private static final String tmpdir  = System.getProperty("java.io.tmpdir");

    @BeforeClass
    public static void init() {
        Assert.assertNotNull(tmpdir);
        String xmlFolderPath = tmpdir;
        if (!xmlFolderPath.endsWith(System.getProperty("file.separator"))) {
            xmlFolderPath = new StringBuilder(xmlFolderPath).append(System.getProperty("file.separator")).toString();
        }
        xmlFolderPath = xmlFolderPath + "cobarSysAdminUT";
        File xmlFolder = new File(xmlFolderPath);
        try {
            if (xmlFolder.exists()) {
                if (!(xmlFolder.isDirectory())) {
                    if (!(xmlFolder.delete())) {
                        logger.error("A none directory file exists and is deleted error!");
                        Assert.fail();
                    }
                } else {
                    xmlPath = xmlFolderPath;
                }
            } else if (xmlFolder.mkdir()) {
                // xmlPath is set after folder has created
                xmlPath = xmlFolderPath;
            } else {
                logger.error("mkdir for SystemAdmin test failed");
                Assert.fail();
            }

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            Assert.fail();
        }
    }

    @AfterClass
    public static void end() {
        File xmlFolder = null;
        if (null != xmlPath) {
            xmlFolder = new File(xmlPath);
            if (!deleteFile(xmlFolder)) {
                Assert.fail();
            }
        }
    }

    // delete folder
    public static boolean deleteFile(File file) {
        boolean success = true;
        String filePath = file.getPath();
        if (file.isDirectory()) {
            String[] children = file.list();
            if (!(null == children || 0 >= children.length)) {
                for (String child : children) {
                    String childFilePath = filePath + System.getProperty("file.separator") + child;
                    File childFile = new File(childFilePath);
                    if (!(deleteFile(childFile))) {
                        success = false;
                    }
                }
            }
        }

        if (success && file.delete()) {
            success = true;
        } else {
            logger.error(filePath + " delete error");
            success = false;
        }
        return success;
    }

}
