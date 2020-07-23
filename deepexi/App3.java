package com.deepexi;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.text.csv.CsvReadConfig;
import cn.hutool.core.text.csv.CsvUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveAction;
import java.util.stream.Collectors;

public class App3 extends RecursiveAction {

    List<String> codes;
    Connection connection;

    int threshold = 200;

    public App3(List<String> codes, Connection connection) {
        this.codes = codes;
        this.connection = connection;
    }

    public static void main(String[] args) throws InterruptedException {
        long s = System.nanoTime();
        List<String> codes = getCode("C:\\java入职-代码\\train_work\\src\\main\\java\\com\\deepexi\\code.csv");
        Connection connection = getConnection();
        App3 app3 = new App3(codes, connection);
        ForkJoinPool pool = new ForkJoinPool(20);
        ForkJoinTask result = pool.submit(app3);
        result.join();
        long e = System.nanoTime();
        double ts = (e - s) / (1e9);
        System.out.println("t=" + ts + "s" + " and speed=" + (10000.0 / ts));
    }

    public static List<String> getCode(String path) {
        CsvReadConfig csvReadConfig = new CsvReadConfig();
        csvReadConfig.setContainsHeader(true);
        csvReadConfig.setSkipEmptyRows(true);
        return CsvUtil.getReader(csvReadConfig)
                .read(FileUtil.file(path))
                .getRows()
                .parallelStream()
                .map(e -> e.get(0))
                .collect(Collectors.toList());
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

    @Override
    protected void compute() {
        if (codes.size() <= threshold) {
            doAction();
        } else {
            int start = codes.size() / 2;
            App3 left = new App3(codes.subList(0, start), this.connection);
            App3 right = new App3(codes.subList(start, codes.size()), this.connection);
            invokeAll(left, right);
        }
    }

    public void doAction() {
        //System.out.println("do "+i);
        System.out.println(codes.get(0));
        String values = codes.parallelStream()
                .map(e -> String.format("('%s','%s','%s','%s')", UUID.randomUUID().toString(), "cf", "cf_" + e, "0"))
                .collect(Collectors.joining(","));
        String sql = "insert into ec_coupon_unique_code_copy1 (id,coupon_id,unique_code,lock_status) values " + values;
        try {
            PreparedStatement statement = connection.prepareStatement(sql);
            statement.execute();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }
}
