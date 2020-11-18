package com.hsh.service;

import com.hsh.model.dto.HbaseResDTO;

import java.io.IOException;
import java.util.List;

public interface IHbaseService {
    HbaseResDTO getByRowkey(String rowKey) ;

    List<String> getColumns(String rowKey) ;
}
