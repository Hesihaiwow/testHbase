package com.hsh.service.impl;

import com.google.common.collect.Lists;
import com.hsh.config.ConfigurationManager;
import com.hsh.Helper.HbaseConnectionHelper;
import com.hsh.model.dto.HbaseResDTO;
import com.hsh.service.IHbaseService;
import com.hsh.source.Constants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * TODO
 *
 * @author hsh
 * @create 2020年11月17日
 */
@Service
public class HbaseServiceImpl implements IHbaseService {


    @Autowired
    @Qualifier(value = "getHbaseConnectionHelper")
    private HbaseConnectionHelper hbaseConnectionHelper;

    @Override
    public HbaseResDTO getByRowkey(String rowKey)  {

       Connection connection = hbaseConnectionHelper.getConnection();

        String tableName = ConfigurationManager.getProperty(Constants.HBASE_TABLE_NAME);
        String faimlyName = ConfigurationManager.getProperty(Constants.HBASE_FAMILY_NAME);
        String t5 = ConfigurationManager.getProperty(Constants.HBASE_T5_NAME);
        String mr = ConfigurationManager.getProperty(Constants.HBASE_MR_NAME);
        String mt = ConfigurationManager.getProperty(Constants.HBASE_MT_NAME);
        String rd = ConfigurationManager.getProperty(Constants.HBASE_RD_NAME);
        String t30 = ConfigurationManager.getProperty(Constants.HBASE_T30_NAME);


        Table tbl = null;
        try {
            tbl = connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            throw new RuntimeException("创建table连接失败");
        }

        Get get = new Get(rowKey.getBytes());

        get.addFamily(faimlyName.getBytes());
        Result result = null;
        try {
            result = tbl.get(get);
        } catch (IOException e) {
            e.printStackTrace();
        }



        String t5s = Bytes.toString(result.getValue(faimlyName.getBytes(), t5.getBytes()));
        String mrs = Bytes.toString(result.getValue(faimlyName.getBytes(), mr.getBytes()));
        String mts = Bytes.toString(result.getValue(faimlyName.getBytes(), mt.getBytes()));
        String rds = Bytes.toString(result.getValue(faimlyName.getBytes(), rd.getBytes()));
        String t30s = Bytes.toString(result.getValue(faimlyName.getBytes(), t30.getBytes()));

        HbaseResDTO hbaseResDTO = new HbaseResDTO();
        hbaseResDTO.setMr(mrs);
        hbaseResDTO.setMt(mts);
        hbaseResDTO.setRd(rds);
        hbaseResDTO.setT5(t5s);
        hbaseResDTO.setT30(t30s);

        try {
            tbl.close();
        } catch (IOException e) {
            throw new RuntimeException("表连接关闭失败");
        }
        hbaseConnectionHelper.close(connection);

        return hbaseResDTO;
    }

    @Override
    public List<String> getColumns(String rowKey)  {

        Connection connection = hbaseConnectionHelper.getConnection();
        Get get = new Get(Bytes.toBytes(rowKey));

        String tableName = ConfigurationManager.getProperty(Constants.HBASE_TABLE_NAME);
        String faimlyName = ConfigurationManager.getProperty(Constants.HBASE_FAMILY_NAME);
        String t5 = ConfigurationManager.getProperty(Constants.HBASE_T5_NAME);
        String mr = ConfigurationManager.getProperty(Constants.HBASE_MR_NAME);
        String mt = ConfigurationManager.getProperty(Constants.HBASE_MT_NAME);
        String rd = ConfigurationManager.getProperty(Constants.HBASE_RD_NAME);
        String t30 = ConfigurationManager.getProperty(Constants.HBASE_T30_NAME);

        Table tbl = null;
        try {
            tbl = connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            throw new RuntimeException("创建table连接失败");
        }

        Result result = null;
        try {
            result = tbl.get(get);
        } catch (IOException e) {
            e.printStackTrace();
        }

        List<String> list = Lists.newArrayList();
        Map<byte[], byte[]> familyMap = result.getFamilyMap(faimlyName.getBytes());
        for(Map.Entry<byte[], byte[]> entry:familyMap.entrySet()){
            list.add(Bytes.toString(entry.getKey()));
        }

        try {
            tbl.close();
        } catch (IOException e) {
            throw new RuntimeException("表连接关闭失败");
        }
        hbaseConnectionHelper.close(connection);

        return list;
    }


}