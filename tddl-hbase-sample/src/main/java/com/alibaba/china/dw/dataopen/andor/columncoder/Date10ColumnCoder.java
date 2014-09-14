package com.alibaba.china.dw.dataopen.andor.columncoder;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.taobao.ustore.repo.hbase.DefaultColumnCoder;
import com.taobao.ustore.repo.hbase.cursor.BytesUtils;

public class Date10ColumnCoder extends DefaultColumnCoder {

    protected byte[] encodeDateToBytes(Object v) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        String s = format.format((Date) v);
        return BytesUtils.stringToBytes(s, ENCODING);
    }

    protected Object decodeDateFromBytes(byte[] v) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        String dateStr = BytesUtils.getString(v, 0, v.length, ENCODING);
        try {
            return format.parse(dateStr);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    protected Object decodeDateFromString(String v) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        try {
            return format.parse(v);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    protected String encodeDateToString(Object v) {
        if (v == null) {
            StringBuilder sb = new StringBuilder();
            sb.append('\0');
            return sb.toString();
        }
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        String s = format.format((Date) v);
        return s;
    }
}
