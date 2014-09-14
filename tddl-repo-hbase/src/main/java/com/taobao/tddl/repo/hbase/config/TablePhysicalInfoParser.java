package com.taobao.tddl.repo.hbase.config;

import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.common.exception.code.ErrorCode;
import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.common.utils.XmlHelper;
import com.taobao.tddl.optimizer.config.table.HBaseColumnCoder;
import com.taobao.ustore.repo.hbase.DefaultColumnCoder;
import com.taobao.ustore.repo.hbase.RowkeyCoder;
import com.taobao.ustore.repo.hbase.TablePhysicalSchema;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * @author jianxing <jianxing.qx@taobao.com>
 */
public class TablePhysicalInfoParser {

    final static Logger logger = LoggerFactory.getLogger(TablePhysicalInfoParser.class);

    public static Map<String, TablePhysicalSchema> parseAll(InputStream in) {

        Document dom_doc = XmlHelper.createDocument(in, null);

        // SAXReader reader = new SAXReader();
        // org.dom4j.Document dom_doc = reader.read(in);

        // Node tablesNode = dom_doc.getElementsByTagName("tables").item(0);

        NodeList table_nodes = dom_doc.getElementsByTagName("table");
        Map<String, TablePhysicalSchema> schemas = new HashMap<String, TablePhysicalSchema>();
        for (int i = 0; i < table_nodes.getLength(); i++) {
            String tableName = ((Element) table_nodes.item(i)).getAttribute("name").trim();
            tableName = tableName.toUpperCase();
            TablePhysicalSchema ps = parseTable((Element) table_nodes.item(i), tableName);
            schemas.put(tableName, ps);
        }
        return schemas;
    }

    private static TablePhysicalSchema parseTable(Element node, String tableName) {
        TablePhysicalSchema schema = new TablePhysicalSchema();
        NodeList columnNodes = node.getElementsByTagName("column");
        Map<String, String> columns = new HashMap<String, String>();
        Map<String, HBaseColumnCoder> coders = new HashMap<String, HBaseColumnCoder>();
        HBaseColumnCoder defaultColumnCoder = null;
        String defaultColumnCoderClass = null;

        NodeList defaultColumnCoderList = node.getElementsByTagName("default-column-coder");
        if (defaultColumnCoderList != null && defaultColumnCoderList.getLength() != 0) {
            defaultColumnCoderClass = defaultColumnCoderList.item(0).getTextContent().trim();

        }
        if (!TStringUtil.isBlank(defaultColumnCoderClass)) {
            try {
                defaultColumnCoder = (HBaseColumnCoder) Class.forName(defaultColumnCoderClass).newInstance();
            } catch (Exception e) {
                StringBuilder msg = new StringBuilder();
                msg.append("column coder init error, ")
                    .append("table name: ")
                    .append(", classname: ")
                    .append(defaultColumnCoderClass);
                logger.warn(msg.toString(), e);
            }
        }

        if (defaultColumnCoder == null) {
            defaultColumnCoder = new DefaultColumnCoder();
        }
        for (int i = 0; i < columnNodes.getLength(); i++) {
            String name = parseColumnName((Element) columnNodes.item(i));
            String mapping = parseColumnMapping((Element) columnNodes.item(i));
            String coderClass = parseColumnCoder((Element) columnNodes.item(i));
            HBaseColumnCoder coder = null;

            if (!TStringUtil.isBlank(coderClass)) {
                try {
                    coder = (HBaseColumnCoder) Class.forName(coderClass).newInstance();
                } catch (Exception e) {
                    StringBuilder msg = new StringBuilder();
                    msg.append("column coder init error, ")
                        .append("table name: ")
                        .append(tableName)
                        .append(", column name: ")
                        .append(name)
                        .append(", classname: ")
                        .append(coderClass);
                    logger.warn(msg.toString(), e);
                }
            }
            if (coder == null) {
                coder = new DefaultColumnCoder();
            }

            coders.put(name.toUpperCase(), coder);
            columns.put(name.toUpperCase(), mapping.equals("") ? name : mapping);
        }

        NodeList rowKeyList = node.getElementsByTagName("rowkey");
        if (rowKeyList == null || rowKeyList.getLength() == 0) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, "table :" + tableName + " miss row key define");
        }

        String[] rowKey = rowKeyList.item(0).getTextContent().toUpperCase().trim().split(",");
        List<String> rowKeyColumns = Arrays.asList(rowKey);

        RowkeyCoder rowKeyGenerator = null;

        String rowKeyClass = null;
        try {

            NodeList rowKeyGeneratorList = node.getElementsByTagName("rowkey-generator-class");
            if (rowKeyGeneratorList == null || rowKeyGeneratorList.getLength() == 0) {
                throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, "table :" + tableName
                                                                     + " miss row key generator define");
            }

            rowKeyClass = rowKeyGeneratorList.item(0).getTextContent().trim();

            rowKeyGenerator = (RowkeyCoder) Class.forName(rowKeyClass)
                .getConstructor(com.taobao.ustore.repo.hbase.TablePhysicalSchema.class)
                .newInstance(schema);
        } catch (Exception e) {
            StringBuilder msg = new StringBuilder();
            msg.append("row key coder init error, ")
                .append("table name: ")
                .append(tableName)
                .append(", classname: ")
                .append(rowKeyClass);
            logger.warn(msg.toString(), e);
        }

        schema.setTableName(tableName);
        schema.setColumns(columns);
        schema.setRowKey(rowKeyColumns);
        schema.setRowKeyGenerator(rowKeyGenerator);
        schema.setColumnCoders(coders);
        return schema;

    }

    private static String parseColumnName(Element node) {
        return node.getAttribute("name").trim();
    }

    private static String parseColumnMapping(Element node) {
        return node.getAttribute("mapping").trim();
    }

    private static String parseColumnCoder(Element node) {
        return node.getAttribute("coder").trim();
    }
}
