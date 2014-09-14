package com.taobao.tddl.util;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;

import com.taobao.tddl.common.exception.TddlNestableRuntimeException;
import com.taobao.tddl.common.utils.GeneralUtil;

public class SqlFileUtil {

    public static String getSql(String fileName) {
        InputStream input = null;
        try {
            input = GeneralUtil.getInputStream("sql" + File.separator + fileName);
            List<String> lines = IOUtils.readLines(input);
            return StringUtils.join(lines, "\n");
        } catch (IOException e) {
            throw new TddlNestableRuntimeException(e);
        } finally {
            IOUtils.closeQuietly(input);
        }
    }
}
