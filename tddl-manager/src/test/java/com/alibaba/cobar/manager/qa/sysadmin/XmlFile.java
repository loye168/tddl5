package com.alibaba.cobar.manager.qa.sysadmin;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.junit.Assert;

/**
 * @author xiaowen.guoxw
 */

public class XmlFile {

    private String xmlPath;
    private String propertyName;

    public XmlFile(String xmlPath, String propertyName){
        Assert.assertNotNull(xmlPath);
        Assert.assertNotNull(propertyName);

        this.xmlPath = xmlPath;
        this.propertyName = propertyName;
    }

    public void init() throws IOException {
        BufferedWriter bf = null;

        try {
            bf = new BufferedWriter(new FileWriter(new File(this.xmlPath)));
            bf.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n");
            bf.write("<" + this.propertyName + ">\r\n");
            bf.write("</" + this.propertyName + ">");
            bf.flush();
        } finally {
            if (null != bf) {
                bf.close();
            }
        }
    }
}
