package com.nakanara;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Reader;
import java.io.StringWriter;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
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
        String k = "", v = "";
        int type = 0;

        while(rs.next()) {
            data = new HashMap<>();
            t=0;

            for(int i=0; i < columns.size(); i++) {
                k = columns.get(i);
                v = "";
                type = columnType.get(i);

                if(type == Types.CLOB) {
                    Reader r = rs.getCharacterStream(i);
                    if (r != null) {
                        StringWriter w = new StringWriter();
                        int read;
                        while ((read = r.read()) != -1) {
                            w.write(read);
                        }

                        w.flush();
                        v = w.toString();
                    }

                }
                else if(type == Types.DATE) {
                    v =  df_dt.format(rs.getDate(i));
                } else {
                    v = rs.getString(k);
                    data.put(k, v);
                }

                data.put(k, v);
                if(t==0) {
                    logger.debug("{} // Key={} //Value={}", rowidx, k, v);
                }
                t++;
            }
            rowidx++;

            datas.add(data);
        }
    }


}
