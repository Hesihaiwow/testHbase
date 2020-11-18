package com.hsh.controller;

import com.hsh.service.IHbaseService;
import com.hsh.source.ZSYResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

/**
 * TODO
 *
 * @author hsh
 * @create 2020年11月17日
 */
@RestController
@RequestMapping("/hbase")
public class HbaseController {

    @Autowired
    private IHbaseService hbaseService;


    @GetMapping("/get-by-rowkey/{rowKey}")
    public ZSYResult getByRowkey(@PathVariable("rowKey") String rowKey) throws IOException {
        return ZSYResult.success().data(hbaseService.getByRowkey(rowKey));
    }

    @GetMapping("/get-columns/{rowKey}")
    public ZSYResult getColumns(@PathVariable("rowKey") String rowKey) throws IOException {
        return ZSYResult.success().data(hbaseService.getColumns(rowKey));
    }
}