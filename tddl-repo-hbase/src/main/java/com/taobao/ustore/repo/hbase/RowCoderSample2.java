package com.taobao.ustore.repo.hbase;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class RowCoderSample2 extends AbstractRowCoder {

    public RowCoderSample2(TablePhysicalSchema schema){
        super(schema);
    }

    @Override
    public Map<String, Object> decodeRowKey(byte[] rowKey) {
        String rowKeyStr[] = new String(rowKey).split("\\$");

        int id;
        String name;
        Date gmt_create = null;

        id = Integer.valueOf(rowKeyStr[0]);
        name = rowKeyStr[1];

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        try {
            gmt_create = sdf.parse(rowKeyStr[2]);
        } catch (ParseException e) {
        }

        Map<String, Object> rowKeyColumnValues = new HashMap();
        rowKeyColumnValues.put("ID", id);
        rowKeyColumnValues.put("NAME", name);
        rowKeyColumnValues.put("GMT_CREATE", gmt_create);

        return rowKeyColumnValues;

    }

    @Override
    public byte[] encodeRowKey(Map<String, Object> rowKeyColumnValues) {
        StringBuilder rowKeyStr = new StringBuilder();
        Integer id = (Integer) rowKeyColumnValues.get("ID");
        String name = (String) rowKeyColumnValues.get("NAME");
        Date gmt_create = (Date) rowKeyColumnValues.get("GMT_CREATE");

        if (id != null)

        rowKeyStr.append(id);
        else rowKeyStr.append('\0');

        rowKeyStr.append("$");

        if (name != null) rowKeyStr.append(name);
        else rowKeyStr.append('\0');

        rowKeyStr.append("$");

        if (gmt_create != null) rowKeyStr.append(gmt_create.toString());
        else rowKeyStr.append('\0');

        return rowKeyStr.toString().getBytes();
    }

}
