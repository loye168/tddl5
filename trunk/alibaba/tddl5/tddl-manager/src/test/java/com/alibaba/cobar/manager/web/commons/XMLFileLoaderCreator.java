package com.alibaba.cobar.manager.web.commons;

import java.io.File;

import org.junit.Assert;

import com.alibaba.cobar.manager.dao.xml.XMLFileLoader;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

public class XMLFileLoaderCreator implements XMLFileLoader {

    private static final Logger logger  = LoggerFactory.getLogger(XMLFileLoaderCreator.class);
    private final String        tmpdir  = System.getProperty("java.io.tmpdir");
    private String              xmlPath = null;

    @Override
    public String getFilePath() {
        return null == xmlPath ? createFile() : xmlPath;
    }

    public String createFile() {
        Assert.assertNotNull(tmpdir);
        String xmlFolderPath = null;
        if (!tmpdir.endsWith(System.getProperty("file.separator"))) {
            xmlFolderPath = new StringBuilder(tmpdir).append(System.getProperty("file.separator"))
                .append("cobarManagerWebUT")
                .toString();
        } else {
            xmlFolderPath = new StringBuilder(tmpdir).append("cobarManagerWebUT").toString();
        }
        File xmlFolder = new File(xmlFolderPath);
        try {
            if (xmlFolder.exists()) {
                if (!(xmlFolder.isDirectory())) {
                    if (!(xmlFolder.delete())) {
                        logger.error("A none directory file name \"cobarManagerWebUT\" exists and delete error!");
                        Assert.fail();
                    }
                } else {
                    xmlPath = xmlFolderPath;
                    new XmlFile(xmlPath).delete();
                }
            }
            if (xmlFolder.mkdir()) {
                // xmlPath is set after folder has created
                xmlPath = xmlFolderPath;
            } else {
                logger.error("mkdir for cobarManagerWebUT test failed");
                Assert.fail();
            }

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            Assert.fail();
        }
        new XmlFile(xmlPath).initData();
        return xmlPath;
    }

}
