package com.alibaba.cobar.manager.dao.xml;

import java.io.File;
import java.net.URL;

import org.springframework.beans.factory.InitializingBean;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

public class XMLFileLoaderPath implements XMLFileLoader, InitializingBean {

    private static final Logger logger = LoggerFactory.getLogger(XMLFileLoaderPath.class);
    private String              xmlPath;

    public void setXmlPath(String xmlPath) {
        this.xmlPath = xmlPath;
    }

    @Override
    public String getFilePath() {
        return this.xmlPath;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (null == xmlPath) {
            logger.error("xmlpath doesn't set!");
            throw new IllegalArgumentException("xmlPath doesn't set!");
        }

        URL url = XMLFileLoaderPath.class.getClassLoader().getResource(xmlPath);
        if (url != null) {
            File file = new File(url.toURI());
            xmlPath = file.getAbsolutePath();

            if (!xmlPath.endsWith(System.getProperty("file.separator"))) {
                xmlPath = new StringBuilder(xmlPath).append(System.getProperty("file.separator")).toString();
            }
        }
    }

}
