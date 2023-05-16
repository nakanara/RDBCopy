package com.nakanara;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.io.StringWriter;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;

public class MetaData {

    private static final Logger logger = LoggerFactory.getLogger(MetaData.class);

    Vector<String> columns;
    Vector<Integer> columnType;
    List<Map<String, Object>> datas = null;

    public static SimpleDateFormat df_dt = new SimpleDateFormat(
            "yyyyMMddHHmmss");


    public Vector<String> getColumns(){
        return columns;
    }

    public List<Map<String, Object>> getDatas(){
        return datas;
    }

    public int getColumnCount(){
        return columns.size();
    }

    public int getRowSize(){
        return datas.size();
    }

    public void load(ResultSet rs) throws Exception{

        columns = new Vector<>();
        columnType = new Vector<>();
        datas = new LinkedList<>();
        Map<String, Object> data  = null;

        ResultSetMetaData rsd = null;

        rsd = rs.getMetaData();

        int colSize = rsd.getColumnCount();

        for(int i =1 ; i <= colSize; i++) {
            columns.add(rsd.getColumnLabel(i));
            columnType.add(rsd.getColumnType(i));
        }


        int t=0;
        int rowidx = 0;
        String k = "";
        Object v;
        int type = 0;

        while(rs.next()) {
            data = new HashMap<>();
            t=0;

            for(int i=0; i < columns.size(); i++) {
                k = columns.get(i);
                int j = i+1;
                v = "";
                type = columnType.get(i);

                if(type == Types.CLOB) {

                    Reader r = null;
                    try {
                        r = rs.getCharacterStream(j);
                        if(r != null) {
                            v = convertClobToString(r);
                        }
                    } finally {
                        if(r != null) {
                            try {
                                r.close();
                            }catch(Exception e){}
                        }
                    }
                } else if(type == Types.DATE ) {
                    v =  df_dt.format(rs.getDate(j));
                } else if(type == Types.TIMESTAMP) {
                    v = rs.getObject(j);
                } else {
                    v = rs.getString(j);
                }

                data.put(k, v);
                if(t==0) {
                    // logger.debug("{} // Key={} //Value={}", rowidx, k, v);
                }
                t++;
            }
            rowidx++;

            datas.add(data);
        }
    }

    protected String convertClobToString(Reader r){
        StringBuffer v = new StringBuffer();

        try {

            int l_nchars = 0;
            char[] l_buffer = new char[102400]; // 1024 * 100
            while((l_nchars = r.read(l_buffer)) != -1){
                v.append(l_buffer,0,l_nchars);
            }

        }catch(IOException ioe){
            ioe.printStackTrace();
        }

        return v.toString();
    }


}
