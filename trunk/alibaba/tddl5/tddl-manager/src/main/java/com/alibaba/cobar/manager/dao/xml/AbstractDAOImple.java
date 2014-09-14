package com.alibaba.cobar.manager.dao.xml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.io.IOUtils;
import org.xmlpull.mxp1_serializer.MXSerializer;
import org.xmlpull.v1.XmlPullParser;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * @author haiqing.zhuhq 2011-6-14
 */
public abstract class AbstractDAOImple {

    private static final Logger   logger           = LoggerFactory.getLogger(AbstractDAOImple.class);
    protected String              xmlPath;
    protected XmlPullParser       xpp;
    protected MXSerializer        xsl;
    protected XMLFileLoader       xmlFileLoader;
    protected static final long[] EMPTY_LONG_ARRAY = new long[0];
    protected static final int[]  EMPTY_INT_ARRAY  = new int[0];

    public void setXmlFileLoader(XMLFileLoader xmlFileLoader) {
        this.xmlFileLoader = xmlFileLoader;
    }

    // flag means if it is going to write END_TAG
    protected boolean writePrefix(boolean flag) {
        int count = xsl.getDepth();
        if (flag) {
            count--;
        }
        for (int i = 0; i < count; i++) {
            try {
                xsl.text("  ");
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
                return false;
            }
        }
        return true;
    }

    protected boolean writeProperty(String name, String value) {
        try {
            writePrefix(false);
            xsl.startTag(null, "property");
            xsl.attribute(null, "name", name);
            xsl.text(value);
            xsl.endTag(null, "property");
            xsl.text("\n");
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            return false;
        }
        return true;
    }

    /**
     * copy file from xmlpath to newpath
     * 
     * @param xmlpath : the file to copy
     * @param newpath : the file copy to
     * @return true for copy success, or false for fail.
     */
    protected boolean fileCopy(String xmlpath, String newpath) {
        int length = 1024 * 1024; // 1MB
        FileInputStream in = null;
        FileOutputStream out = null;
        try {
            in = new FileInputStream(xmlpath);
            out = new FileOutputStream(newpath);
            FileChannel inC = in.getChannel();
            FileChannel outC = out.getChannel();
            ByteBuffer b = null;
            while (true) {
                if (inC.position() == inC.size()) {
                    inC.close();
                    outC.close();
                    return true;
                }
                if ((inC.size() - inC.position()) < length) {
                    length = (int) (inC.size() - inC.position());
                }
                b = ByteBuffer.allocateDirect(length);
                inC.read(b);
                b.flip();
                outC.write(b);
                outC.force(false);
            }
        } catch (FileNotFoundException e) {
            logger.error(e.getMessage(), e);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        } finally {
            IOUtils.closeQuietly(in);
            IOUtils.closeQuietly(out);
        }
        return false;
    }

    protected boolean backup(String path) {
        Date today = new Date();
        SimpleDateFormat format = new SimpleDateFormat("-yyyy-MM-dd");
        String date = format.format(today);
        String newpath = new StringBuilder(path).append(date).toString();
        File f = new File(newpath);
        if (f.exists()) {
            return true;
        }
        if (fileCopy(path, newpath)) {
            return true;
        }
        logger.error("file backup fail for: " + newpath);
        return false;
    }

    protected boolean recovery(String path) {
        Date today = new Date();
        SimpleDateFormat format = new SimpleDateFormat("-yyyy-MM-dd");
        String date = format.format(today);
        String newpath = new StringBuilder(path).append(date).toString();
        File f = new File(newpath);
        if (f.exists()) {
            if (fileCopy(newpath, path)) {
                return true;
            }
            logger.error("file recovery fail for : " + newpath);
        }
        logger.error("file does exsit for recovery: " + newpath);
        return false;
    }

}
