package com.xunmall.example.flink.streaming.aio;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static java.util.concurrent.Executors.newFixedThreadPool;

/**
 * @author wangyj03@zenmen.com
 * @description
 * @date 2021/1/11 9:47
 */
public class AsyncFunctionForMysql extends RichAsyncFunction<User, AsyncUser> {

    private static Logger logger = LoggerFactory.getLogger(AsyncFunctionForMysql.class);

    private transient MysqlClient client;

    private transient ExecutorService executorService;

    @Override
    public void open(Configuration parameters) throws Exception {
        logger.info("async function for mysql open ...");
        super.open(parameters);
        client = new MysqlClient();
        executorService = newFixedThreadPool(30);
    }

    @Override
    public void close() throws Exception {
        logger.info("async function for mysql close ...");
        super.close();
    }

    @Override
    public void asyncInvoke(User input, ResultFuture<AsyncUser> resultFuture) throws Exception {
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                System.out.println("submit query : " + input.getId() + "-1-" + System.currentTimeMillis());
                AsyncUser tmp = client.queryUser(input);
                resultFuture.complete(Collections.singleton(tmp));
            }
        });
    }

    @Override
    public void timeout(User input, ResultFuture<AsyncUser> resultFuture) throws Exception {
        logger.warn("async function for mysql timeout");
        List<AsyncUser> list = new ArrayList<>();
        AsyncUser asyncUser = new AsyncUser(input.getId(), input.getUsername(), input.getPassword(), "timeout");
        list.add(asyncUser);
        resultFuture.complete(list);
    }
}
