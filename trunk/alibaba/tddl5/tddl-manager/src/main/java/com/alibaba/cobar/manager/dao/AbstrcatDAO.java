package com.alibaba.cobar.manager.dao;

/**
 * @author haiqing.zhuhq 2011-6-14
 */
public interface AbstrcatDAO {

    boolean writePrefix(boolean flag);

    boolean writeProperty(String name, String value);

    boolean fileCopy(String xmlpath, String newpath);

    boolean backup(String date, String path);

    boolean recovery(final String date, String path);

    boolean recovery();
}
