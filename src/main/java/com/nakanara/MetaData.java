package com.nakanara;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.*;

public class MetaData {

    private static final Logger logger = LoggerFactory.getLogger(MetaData.class);

    Vector<String> columns;
    List<Map<String, Object>> datas = null;


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
        datas = new LinkedList<>();
        Map<String, Object> data  = null;

        ResultSetMetaData rsd = null;

        rsd = rs.getMetaData();

        int colSize = rsd.getColumnCount();

        for(int i =1 ; i <= colSize; i++) {
            columns.add(rsd.getColumnLabel(i));
        }


        int t=0;
        int rowidx = 0;
        while(rs.next()) {
            data = new HashMap<>();
            t=0;

            for(String k : columns) {
                Object v = rs.getObject(k);
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
