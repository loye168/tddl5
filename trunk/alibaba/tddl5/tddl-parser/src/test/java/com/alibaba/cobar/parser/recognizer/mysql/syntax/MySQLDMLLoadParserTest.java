/*
 * Copyright 1999-2012 Alibaba Group.
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * (created at 2011-5-18)
 */
package com.alibaba.cobar.parser.recognizer.mysql.syntax;

import java.sql.SQLSyntaxErrorException;

import org.junit.Assert;

import com.alibaba.cobar.parser.ast.stmt.SQLStatement;
import com.alibaba.cobar.parser.recognizer.SQLParserDelegate;

/**
 * @author mengshi.sunmengshi 2014年5月13日 下午6:01:38
 * @since 5.1.0
 */
public class MySQLDMLLoadParserTest extends AbstractSyntaxTest {

    public void testLoadData() throws SQLSyntaxErrorException {
        String sql = "LOAD DATA INFILE 'd:/data.txt' INTO TABLE XXXX";
        SQLStatement stmt = SQLParserDelegate.parse(sql);
        String output = output2MySQL(stmt, sql);
        Assert.assertEquals("LOAD DATA INFILE 'd:/data.txt' INTO TABLE XXXX", output);

    }

    public void testLoadDataLocal() throws SQLSyntaxErrorException {
        String sql = "LOAD DATA LOCAL INFILE 'd:/data.txt' INTO TABLE XXXX";
        SQLStatement stmt = SQLParserDelegate.parse(sql);
        String output = output2MySQL(stmt, sql);
        Assert.assertEquals("LOAD DATA LOCAL INFILE 'd:/data.txt' INTO TABLE XXXX", output);

    }

    public void testLoadDataMode() throws SQLSyntaxErrorException {
        {
            String sql = "LOAD DATA LOW_PRIORITY INFILE 'd:/data.txt' INTO TABLE XXXX";
            SQLStatement stmt = SQLParserDelegate.parse(sql);
            String output = output2MySQL(stmt, sql);
            Assert.assertEquals("LOAD DATA LOW_PRIORITY INFILE 'd:/data.txt' INTO TABLE XXXX", output);
        }

        {
            String sql = "LOAD DATA CONCURRENT LOCAL INFILE 'd:/data.txt' INTO TABLE XXXX";
            SQLStatement stmt = SQLParserDelegate.parse(sql);
            String output = output2MySQL(stmt, sql);
            Assert.assertEquals("LOAD DATA CONCURRENT LOCAL INFILE 'd:/data.txt' INTO TABLE XXXX", output);
        }

        {
            String sql = "LOAD DATA CONCURRENT LOCAL INFILE 'd:/data.txt' IGNORE INTO TABLE XXXX";
            SQLStatement stmt = SQLParserDelegate.parse(sql);
            String output = output2MySQL(stmt, sql);
            Assert.assertEquals("LOAD DATA CONCURRENT LOCAL INFILE 'd:/data.txt' IGNORE INTO TABLE XXXX", output);
        }

        {
            String sql = "LOAD DATA CONCURRENT LOCAL INFILE 'd:/data.txt' REPLACE INTO TABLE XXXX";
            SQLStatement stmt = SQLParserDelegate.parse(sql);
            String output = output2MySQL(stmt, sql);
            Assert.assertEquals("LOAD DATA CONCURRENT LOCAL INFILE 'd:/data.txt' REPLACE INTO TABLE XXXX", output);
        }

    }

    public void testLoadDataCharset() throws SQLSyntaxErrorException {

        {
            String sql = "LOAD DATA CONCURRENT LOCAL INFILE 'd:/data.txt' REPLACE INTO TABLE XXXX CHARACTER SET utf8";
            SQLStatement stmt = SQLParserDelegate.parse(sql);
            String output = output2MySQL(stmt, sql);
            Assert.assertEquals("LOAD DATA CONCURRENT LOCAL INFILE 'd:/data.txt' REPLACE INTO TABLE XXXXCHARACTER SET utf8 ",
                output);
        }

    }

    public void testLoadDataColumns() throws SQLSyntaxErrorException {

        {
            String sql = "LOAD DATA CONCURRENT LOCAL INFILE 'd:/data.txt' REPLACE INTO TABLE XXXX CHARACTER SET utf8 FIELDS TERMINATED BY '\\t' ENCLOSED BY '' ESCAPED BY '\\\\'";
            SQLStatement stmt = SQLParserDelegate.parse(sql);
            String output = output2MySQL(stmt, sql);
            Assert.assertEquals("LOAD DATA CONCURRENT LOCAL INFILE 'd:/data.txt' REPLACE INTO TABLE XXXXCHARACTER SET utf8 COLUMNS TERMINATED BY '\\t' ENCLOSED BY '' ESCAPED BY '\\\\' ",
                output);
        }

    }

    public void testLoadDataLines() throws SQLSyntaxErrorException {

        {
            String sql = "LOAD DATA CONCURRENT LOCAL INFILE 'd:/data.txt' REPLACE INTO TABLE XXXX CHARACTER SET utf8 FIELDS TERMINATED BY '\\t' ENCLOSED BY '' ESCAPED BY '\\\\' LINES STARTING BY '' TERMINATED BY '\\n' ";
            SQLStatement stmt = SQLParserDelegate.parse(sql);
            String output = output2MySQL(stmt, sql);
            Assert.assertEquals("LOAD DATA CONCURRENT LOCAL INFILE 'd:/data.txt' REPLACE INTO TABLE XXXXCHARACTER SET utf8 COLUMNS TERMINATED BY '\\t' ENCLOSED BY '' ESCAPED BY '\\\\' LINESSTARTING BY '' TERMINATED BY '\\n' ",
                output);
        }

    }

    public void testLoadDataIgnoreNumber() throws SQLSyntaxErrorException {

        {
            String sql = "LOAD DATA CONCURRENT LOCAL INFILE 'd:/data.txt' REPLACE INTO TABLE XXXX CHARACTER SET utf8 FIELDS TERMINATED BY '\\t' ENCLOSED BY '' ESCAPED BY '\\\\' LINES STARTING BY '' TERMINATED BY '\\n' ignore 2 lines";
            SQLStatement stmt = SQLParserDelegate.parse(sql);
            String output = output2MySQL(stmt, sql);
            Assert.assertEquals("LOAD DATA CONCURRENT LOCAL INFILE 'd:/data.txt' REPLACE INTO TABLE XXXXCHARACTER SET utf8 COLUMNS TERMINATED BY '\\t' ENCLOSED BY '' ESCAPED BY '\\\\' LINESSTARTING BY '' TERMINATED BY '\\n' IGNORE 2 LINES ",
                output);
        }

    }

    public void testLoadDataColumnsSet() throws SQLSyntaxErrorException {

        {
            String sql = "LOAD DATA CONCURRENT LOCAL INFILE 'd:/data.txt' REPLACE INTO TABLE XXXX CHARACTER SET utf8 FIELDS TERMINATED BY '\\t' ENCLOSED BY '' ESCAPED BY '\\\\' LINES STARTING BY '' TERMINATED BY '\\n' ignore 2 lines (id,name,address)";
            SQLStatement stmt = SQLParserDelegate.parse(sql);
            String output = output2MySQL(stmt, sql);
            Assert.assertEquals("LOAD DATA CONCURRENT LOCAL INFILE 'd:/data.txt' REPLACE INTO TABLE XXXXCHARACTER SET utf8 COLUMNS TERMINATED BY '\\t' ENCLOSED BY '' ESCAPED BY '\\\\' LINESSTARTING BY '' TERMINATED BY '\\n' IGNORE 2 LINES (ID, NAME, ADDRESS) ",
                output);
        }

    }

    public void testLoadDataValues() throws SQLSyntaxErrorException {

        {
            String sql = "LOAD DATA CONCURRENT LOCAL INFILE 'd:/data.txt' REPLACE INTO TABLE XXXX CHARACTER SET utf8 FIELDS TERMINATED BY '\\t' ENCLOSED BY '' ESCAPED BY '\\\\' LINES STARTING BY '' TERMINATED BY '\\n' ignore 2 lines (id,name,address) set pk=1,pk2=now()";
            SQLStatement stmt = SQLParserDelegate.parse(sql);
            String output = output2MySQL(stmt, sql);
            Assert.assertEquals("LOAD DATA CONCURRENT LOCAL INFILE 'd:/data.txt' REPLACE INTO TABLE XXXXCHARACTER SET utf8 COLUMNS TERMINATED BY '\\t' ENCLOSED BY '' ESCAPED BY '\\\\' LINESSTARTING BY '' TERMINATED BY '\\n' IGNORE 2 LINES (ID, NAME, ADDRESS) SET PK = 1, PK2 = NOW()",
                output);
        }

    }
}
