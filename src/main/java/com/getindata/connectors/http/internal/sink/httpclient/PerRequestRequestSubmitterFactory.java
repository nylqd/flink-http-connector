package com.getindata.connectors.http.internal.sink.httpclient;

import com.getindata.connectors.http.internal.utils.OkHttpClientFactory;
import com.getindata.connectors.http.internal.utils.ThreadUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PerRequestRequestSubmitterFactory implements RequestSubmitterFactory {

    // TODO Add this property to config. Make sure to add note in README.md that will describe that
    //  any value greater than one will break order of messages.
    int HTTP_CLIENT_THREAD_POOL_SIZE = 1;

    @Override
    public RequestSubmitter createSubmitter(Properties properties, Map<String, String> headerMap) {

        ExecutorService httpClientExecutor =
            Executors.newFixedThreadPool(
                HTTP_CLIENT_THREAD_POOL_SIZE,
                new ExecutorThreadFactory(
                    "http-sink-client-per-request-worker", ThreadUtils.LOGGING_EXCEPTION_HANDLER));

        return new PerRequestSubmitter(
                properties,
                headerMap,
                OkHttpClientFactory.createClient(properties, httpClientExecutor)
            );
    }
}
