package com.alibaba.cobar.server.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropertiesUtil {

    private static final Logger logger = LoggerFactory.getLogger(PropertiesUtil.class);

    public static Properties loadPropertiesFromFile(String filePath) {
        Properties props = new Properties();
        InputStream input;
        try {
            input = new FileInputStream(filePath);
            props.load(input);
            input.close();
        } catch (Exception e) {
            logger.error("[corona-prop],load prop file error, file path:" + filePath, e);
            return null;
        }

        return props;
    }

    public static Properties convertString2Properties(String str) {
        if (str != null) {
            Properties props = new Properties();
            StringReader reader = new StringReader(str);
            try {
                props.load(reader);
                return props;
            } catch (IOException e) {
                logger.error("[corona-prop], convert string to properties error, string:" + str, e);
                return null;
            } finally {
                reader.close();
            }
        }
        return null;
    }
}
