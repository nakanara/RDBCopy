package com.nakanara;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 다른 종류의 RDB 복사때문에 생성
 */
public class RDBCopy {


    private static final Logger logger = LoggerFactory.getLogger(RDBCopy.class);

    public static void main(String args[]) {


        String s_driver = "oracle.jdbc.OracleDriver";
        String s_url    = "jdbc:oracle:thin:@localhost:1521/source";
        String s_user   = "user";
        String s_pw     = "pw";

        String t_driver = "com.mysql.jdbc.Driver";
        String t_url    = "jdbc:mysql://localhost:3306/target";
        String t_user   = "user";
        String t_pw     = "pw";

        String search_sql = "select * from source_table";;
        String target_table = "target_table";

        // 원본 소스 정보
        DataSource source = new DataSource(s_driver, s_url, s_user, s_pw);
        logger.info("Source Info \n\tDriver={}\n\tUrl={}\n\tUser={}\n\tPw={}", s_driver, s_url, s_user, s_pw);

        // 복사 대상
        DataSource target = new DataSource(t_driver, t_url, t_user, t_pw);
        logger.info("Target Info \n\tDriver={}\n\tUrl={}\n\tUser={}\n\tPw={}", t_driver, t_url, t_user, t_pw);

        MetaData md =  source.select(search_sql);

        target.insert(target_table,  md);

        target.close();
        source.close();

    }


}

