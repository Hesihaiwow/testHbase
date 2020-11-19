package com.hsh.Helper;

import com.hsh.config.ConfigurationManager;
import com.hsh.source.Constants;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.LinkedList;


/**
 * TODO
 *
 * @author hsh
 * @create 2020年11月17日
 */
@Configuration
public class HbaseConnectionHelper {


    private static org.apache.hadoop.conf.Configuration conf;
    private static String tableName;
    private static LinkedList<Connection> connections = new LinkedList<>();

    static {
        org.apache.hadoop.conf.Configuration HBASE_CONFIG = new org.apache.hadoop.conf.Configuration();

        String quorum = ConfigurationManager.getProperty(Constants.HBASE_ZOOKEEPER_QUORUM);
        HBASE_CONFIG.set(Constants.HBASE_ZOOKEEPER_QUORUM,quorum);

        String port = ConfigurationManager.getProperty(Constants.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT);
        HBASE_CONFIG.set(Constants.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT,port);

        conf = HBaseConfiguration.create(HBASE_CONFIG);
        tableName = ConfigurationManager.getProperty(Constants.HBASE_TABLE_NAME);

        for(int i = 0; i < 100; i++){
            try {
                Connection connection = ConnectionFactory.createConnection(conf);
                connections.add(connection);
            } catch (IOException e) {
                throw new RuntimeException("创建连接失败");
            }
        }

    }

    public synchronized Connection getConnection(){
        if(connections.size() < 0){
            for(int i = 0; i < 10; i++){
                try {
                    Connection connection = ConnectionFactory.createConnection(conf);
                    connections.add(connection);
                } catch (IOException e) {
                    throw new RuntimeException("创建连接失败");
                }
            }
        }
        return connections.remove();
    }

    public synchronized void close(Connection connection){
        if(connections.size() < 100){
            connections.add(connection);
        }else {
            try {
                connection.close();
            } catch (IOException e) {
               throw new RuntimeException("connection关闭失败");
            }
        }

    }



//    @Bean
//    public Connection getHbaseConnection(){
//        // 判断
//        try {
//            for(int i = 0; i < 10 ; i++ ){
//
//            }
//            return ConnectionFactory.createConnection(conf);
//        } catch (IOException e) {
//           throw new RuntimeException("connection创建失败");
//        }
//    }

//    @Bean
//    public Table getHbaseTable(@Autowired Connection connection){
//        try {
//            return connection.getTable(TableName.valueOf(tableName));
//        } catch (IOException e) {
//            throw new RuntimeException("table创建失败");
//        }
//    }
}