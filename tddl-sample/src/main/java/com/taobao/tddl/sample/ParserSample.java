package com.taobao.tddl.sample;

import java.sql.SQLSyntaxErrorException;

import com.alibaba.cobar.parser.ast.stmt.SQLStatement;
import com.alibaba.cobar.parser.recognizer.SQLParserDelegate;
import com.alibaba.cobar.parser.visitor.MySQLOutputASTVisitor;

/**
 * @author mengshi.sunmengshi 2014年4月24日 下午3:25:27
 * @since 5.1.0
 */
public class ParserSample {

    public static void main(String args[]) throws SQLSyntaxErrorException {
        String orignSql = "UPDATE P_DRUG_INFO_MANAGE A SET A.CRT_IC_NAME = (SELECT I.USER_NAME FROM P_USER_PERSON_INFO I WHERE A.CRT_IC_CODE = I.USER_PERSON_INFO_ID)";
        // String orignSql =
        // "select * from P_DRUG_INFO_MANAGE A where A.CRT_IC_NAME = (SELECT I.USER_NAME FROM P_USER_PERSON_INFO I WHERE A.CRT_IC_CODE = I.USER_PERSON_INFO_ID)";

        orignSql = "select cast(11 as unsigned integer)";
        orignSql = "select cast(11 as signed int)";
        SQLStatement stmt = SQLParserDelegate.parse(orignSql);

        StringBuilder outPutBuilder = new StringBuilder();
        MySQLOutputASTVisitor outPutVisitor = new MySQLOutputASTVisitor(outPutBuilder);
        stmt.accept(outPutVisitor);

        System.out.println(outPutBuilder.toString());

    }

}
