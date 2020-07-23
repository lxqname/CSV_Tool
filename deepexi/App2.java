package com.deepexi;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.text.csv.CsvReadConfig;
import cn.hutool.core.text.csv.CsvUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Hello world!
 */
public class App2 {
    public static void main(String[] args) {
        AtomicInteger cnt = new AtomicInteger();
        long s = System.nanoTime();
        Connection connection = getConnection();
        String format =
                "insert into ec_coupon_unique_code (id,coupon_id,unique_code,lock_status) values ('%s','%s','%s','%s')";
        CsvReadConfig csvReadConfig = new CsvReadConfig();
        csvReadConfig.setContainsHeader(true);
        csvReadConfig.setSkipEmptyRows(true);
        CsvUtil.getReader(csvReadConfig)
                .read(FileUtil.file("C:\\Users\\Addison\\Desktop\\code.csv"))
                .getRows()
                .parallelStream()
                .map(e -> e.get(0))
                .map(e -> String.format(format, UUID.randomUUID().toString(), "cf", "cf_" + e, "0"))
                .map(e -> {
                    try {
                        return connection.prepareStatement(e);
                    } catch (SQLException ex) {
                        ex.printStackTrace();
                    }
                    return null;
                }).filter(e -> e != null).forEach(e -> {
            int i = cnt.incrementAndGet();
            long t = System.nanoTime() - s;
            double ts = t / (1e9);
            //System.out.println(Thread.currentThread().getName()+" : no="+i+" and t="+ts+"s"+" and speed="+(i/ts));
            try {
                e.execute();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        });
        long e = System.nanoTime();
        double ts = (e - s) / (1e9);
        System.out.println(ts);

    }

    public static Connection getConnection() {
        Connection connection = null;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            try {
                connection = DriverManager.getConnection("jdbc:mysql://203.195.136.11:3306/dev_tt_equity_center?useUnicode=true&allowMultiQueries=true&characterEncoding=utf-8&useSSL=false&serverTimezone=GMT%2B8", "root", "cdpwy123");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return connection;
    }

    public List<String> getCode(String path) {
        return CsvUtil.getReader()
                .read(FileUtil.file(path))
                .getRows()
                .parallelStream()
                .map(e -> e.getByName("coupon_code"))
                .collect(Collectors.toList());
    }

}
