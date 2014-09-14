package com.taobao.ustore.repo.hbase;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class RowCoderSample3 extends AbstractRowCoder {

    public RowCoderSample3(TablePhysicalSchema schema){
        super(schema);
    }

    @Override
    public Map<String, Object> decodeRowKey(byte[] rowKey) {
        String rowKeyStr[] = new String(rowKey).split("\4");

        String host;
        String type;
        String url;
        Date gmt_create = null;

        host = rowKeyStr[0];

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        try {
            gmt_create = sdf.parse(rowKeyStr[1]);
        } catch (ParseException e) {
        }

        type = rowKeyStr[2];
        url = rowKeyStr[3];

        Map<String, Object> rowKeyColumnValues = new HashMap();
        rowKeyColumnValues.put("HOST", host);
        rowKeyColumnValues.put("URL", url);
        rowKeyColumnValues.put("TYPE", type);
        rowKeyColumnValues.put("GMT_CREATE", gmt_create);

        return rowKeyColumnValues;

    }

    @Override
    public byte[] encodeRowKey(Map<String, Object> rowKeyColumnValues) {
        StringBuilder rowKeyStr = new StringBuilder();
        String host = (String) rowKeyColumnValues.get("HOST");
        Date gmt_create = (Date) rowKeyColumnValues.get("GMT_CREATE");
        String type = (String) rowKeyColumnValues.get("TYPE");
        String url = (String) rowKeyColumnValues.get("URL");

        if (host != null) rowKeyStr.append(host);
        else rowKeyStr.append('\0');

        rowKeyStr.append((char) 4);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

        String gmt_create_str = sdf.format(gmt_create);
        if (gmt_create != null) rowKeyStr.append(gmt_create_str);
        else rowKeyStr.append('\0');

        rowKeyStr.append((char) 4);

        if (type != null) rowKeyStr.append(type);
        else rowKeyStr.append('\0');

        rowKeyStr.append((char) 4);

        if (url != null) rowKeyStr.append(url);
        else rowKeyStr.append('\0');
        return rowKeyStr.toString().getBytes();
    }

}
