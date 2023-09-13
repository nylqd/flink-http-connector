package com.getindata.connectors.http.internal.sink.httpclient;

import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;
import com.getindata.connectors.http.internal.utils.ThreadUtils;
import okhttp3.OkHttpClient;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class AbstractRequestSubmitter implements RequestSubmitter {

    protected static final int HTTP_CLIENT_PUBLISHING_THREAD_POOL_SIZE = 1;

    protected static final String DEFAULT_REQUEST_TIMEOUT_SECONDS = "30";

    /**
     * Thread pool to handle HTTP response from HTTP client.
     */
    protected final ExecutorService publishingThreadPool;

    protected final int httpRequestTimeOutSeconds;

    protected final Map<String, String> headerMap;

    protected final OkHttpClient okHttpClient;

    public AbstractRequestSubmitter(
            Properties properties,
            Map<String, String> headerMap,
            OkHttpClient okHttpClient) {

        this.headerMap = headerMap;
        this.publishingThreadPool =
            Executors.newFixedThreadPool(
                HTTP_CLIENT_PUBLISHING_THREAD_POOL_SIZE,
                new ExecutorThreadFactory(
                    "http-sink-client-response-worker", ThreadUtils.LOGGING_EXCEPTION_HANDLER));

        this.httpRequestTimeOutSeconds = Integer.parseInt(
            properties.getProperty(HttpConnectorConfigConstants.SINK_HTTP_TIMEOUT_SECONDS,
                DEFAULT_REQUEST_TIMEOUT_SECONDS)
        );

        this.okHttpClient = okHttpClient;
    }
}
