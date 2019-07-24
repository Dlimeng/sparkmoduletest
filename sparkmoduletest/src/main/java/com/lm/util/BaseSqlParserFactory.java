package com.lm.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Author: limeng
 * @Date: 2019/7/23 18:25
 */
public class BaseSqlParserFactory {
    private static Logger logger = LoggerFactory.getLogger(BaseSqlParserFactory.class);
    /**
     * 查询
     */
    private static final String SELECT1 ="(select)(.+)(from)(.+)";

    private static final String SAVE1 = "(insert)(.+)(select)";
    private static final String DELETE1="(delete from)(.+)";
    private static final String CREATE1="(create)(.+)";
    private static final String TRUNCATE1="(truncate)(.+)";

    private static final String source="hive";

    public static Boolean generateParser(String originalSql) {

        if( contains(originalSql,SELECT1)) {
            return true;
        }else if(contains(originalSql,SAVE1)){
            return true;
        }else if(contains(originalSql,DELETE1)){
            return true;
        }else if( contains(originalSql,CREATE1)){
            return true;
        }else if(contains(originalSql,TRUNCATE1)){
            return true;
        }else{
            logger.info("BaseSqlParserFactory generateParser error=>sql:{}",originalSql);
            return false;
        }
    }


    /**
     * 包含
     * @param sql sql
     * @param regExp 正则
     * @return
     */
    private static boolean contains(String sql,String regExp){
        Pattern pattern=Pattern.compile(regExp,Pattern.CASE_INSENSITIVE);
        Matcher matcher=pattern.matcher(sql);
        return matcher.find();
    }

    public static void main(String[] args) {
        generateParser("select * from ssss");
        generateParser("insert into a_mart.A_O02_STK_CODE_FS select * from  b_mart.B_O02_STK_CODE_FS;");
        generateParser("create   table IF NOT EXISTS a_mart.A_O02_STK_CODE_FS ");
        generateParser("truncate table b_mart.B_B04_COMP_TAG;");
    }


}
