package com.nakanara;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Map;

public class DataSource {

    private String driverName;
    private String url;
    private String user;
    private String password;

    private Connection connection;

    private Logger logger = LoggerFactory.getLogger(DataSource.class);

    public DataSource(String p_driver, String p_url, String p_user, String p_password) {

        driverName  = p_driver;     //"oracle.jdbc.driver.OracleDriver";
        url         = p_url;        //"oracle:thin:localhost:1521:ORCL";
        user        = p_user;       //"scott";
        password    = p_password;   // "tiger";

        getConnection();
    }

    public Connection getConnection(){
        try {
            Class.forName(driverName);

            connection = DriverManager.getConnection(url, user, password);

            return connection;
        }catch (ClassNotFoundException | SQLException e){
            e.printStackTrace();
        }
        return null;
    }

    public MetaData select(String sql){

        Statement statement = null;
        ResultSet rs = null;


        MetaData metaData = new MetaData();

        logger.info("select sql={}", sql);

        try {
            statement = connection.createStatement();

            rs = statement.executeQuery(sql);

            metaData.load(rs);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                rs.close();
            }catch(Exception e){}

            try {
                statement.close();
            }catch(Exception e){}
        }

        logger.info("Search Column Count={}", metaData.getColumnCount());
        logger.info("Search Row Size={}", metaData.getRowSize());

        return metaData;
    }

    public void insert(String table, MetaData md)  {
        StringBuffer buf = new StringBuffer();

        buf.append("INSERT INTO ").append(table).append("(");
        int i=0;
        for(String k : md.getColumns()) {
            if(i > 0) buf.append(", ");

            buf.append(k);
            i++;
        }

        buf.append(") VALUES (");

        for(i=0; i<  md.getColumnCount(); i++) {
            if(i > 0) buf.append(", ");

            buf.append("?");
        }

        buf.append(")");

        logger.info("Insert sql=\n{}", buf.toString());

        // 데이터 입력
        PreparedStatement ps = null;

        try {
            ps = connection.prepareStatement(buf.toString());

            for(Map<String, Object> data : md.getDatas()){
                i=1;
                for (String k : md.getColumns()) {
                    ps.setObject(i, data.get(k));
                    i++;
                }

                ps.addBatch();
            }

            ps.executeBatch();

        }catch(SQLException e){
            e.printStackTrace();
            logger.error("Sql Exception={}", e);
        }finally {
            try {
                ps.close();
            }catch(Exception e){
                logger.error("Error={}", e);
            }
        }
    }

    public void close(){

        try {
            this.connection.close();
        } catch (SQLException e) {
            logger.error("Close Error={}", e);
        }
    }
}
