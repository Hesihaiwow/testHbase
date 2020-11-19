package com.hsh.config;

import com.hsh.Helper.HbaseConnectionHelper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * TODO
 *
 * @author hsh
 * @create 2020年11月19日
 */

@Configuration
public class ConnectionConfig {

    @Bean
    public HbaseConnectionHelper getHbaseConnectionHelper(){
        HbaseConnectionHelper hbaseConnectionHelper = new HbaseConnectionHelper();
        return hbaseConnectionHelper;
    }
}