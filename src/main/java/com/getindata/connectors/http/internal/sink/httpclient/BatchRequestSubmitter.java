package com.getindata.connectors.http.internal.sink.httpclient;

import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;
import com.getindata.connectors.http.internal.sink.HttpSinkRequestEntry;
import lombok.extern.slf4j.Slf4j;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import org.apache.flink.annotation.VisibleForTesting;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants.HEADER_REQUEST_TIMEOUT_SECONDS;

/**
 * This implementation groups received events in batches and submits each batch as individual HTTP
 * requests. Batch is created based on batch size or based on HTTP method type.
 */
@Slf4j
public class BatchRequestSubmitter extends AbstractRequestSubmitter {

    private static final byte[] BATCH_START_BYTES = "[".getBytes(StandardCharsets.UTF_8);

    private static final byte[] BATCH_END_BYTES = "]".getBytes(StandardCharsets.UTF_8);

    private static final byte[] BATCH_ELEMENT_DELIM_BYTES = ",".getBytes(StandardCharsets.UTF_8);

    private final int httpRequestBatchSize;

    public BatchRequestSubmitter(
            Properties properties,
            Map<String, String> headerMap,
            OkHttpClient okHttpClient) {

        super(properties, headerMap, okHttpClient);

        this.httpRequestBatchSize = Integer.parseInt(
            properties.getProperty(HttpConnectorConfigConstants.SINK_HTTP_BATCH_REQUEST_SIZE)
        );
    }

    @Override
    public List<CompletableFuture<OkHttpResponseWrapper>> submit(
            String endpointUrl,
            List<HttpSinkRequestEntry> requestsToSubmit) {

        if (requestsToSubmit.isEmpty()) {
            return Collections.emptyList();
        }

        ArrayList<CompletableFuture<OkHttpResponseWrapper>> responseFutures = new ArrayList<>();
        String previousReqeustMethod = requestsToSubmit.get(0).method;
        List<HttpSinkRequestEntry> requestBatch = new ArrayList<>(httpRequestBatchSize);

        for (HttpSinkRequestEntry entry : requestsToSubmit) {
            if (requestBatch.size() == httpRequestBatchSize
                || !previousReqeustMethod.equalsIgnoreCase(entry.method)) {
                // break batch and submit
                responseFutures.add(sendBatch(endpointUrl, requestBatch));
                requestBatch.clear();
            }
            requestBatch.add(entry);
            previousReqeustMethod = entry.method;
        }

        // submit anything that left
        responseFutures.add(sendBatch(endpointUrl, requestBatch));
        return responseFutures;
    }

    @VisibleForTesting
    int getBatchSize() {
        return httpRequestBatchSize;
    }

    private CompletableFuture<OkHttpResponseWrapper> sendBatch(
            String endpointUrl,
            List<HttpSinkRequestEntry> reqeustBatch) {

        HttpRequest httpRequest = buildHttpRequest(reqeustBatch, endpointUrl);

        OkHttpResponseFuture callback = new OkHttpResponseFuture();
        okHttpClient.newCall(httpRequest.getRequest()).enqueue(callback);

        return callback.future
                .exceptionally(ex -> {
                    // TODO This will be executed on a ForkJoinPool Thread... refactor this someday.
                    log.error("Request fatally failed because of an exception", ex);
                    return null;
                })
                .thenApplyAsync(
                        res -> new OkHttpResponseWrapper(httpRequest, res),
                        publishingThreadPool
                );
    }

    private HttpRequest buildHttpRequest(List<HttpSinkRequestEntry> reqeustBatch, String endpointUrl) {

        try {
            String method = reqeustBatch.get(0).method;
            List<byte[]> elements = new ArrayList<>(reqeustBatch.size());

            // By default, Java's BodyPublishers.ofByteArrays(elements) will just put Jsons
            // into the HTTP body without any context.
            // What we do here is we pack every Json/byteArray into Json Array hence '[' and ']'
            // at the end, and we separate every element with comma.
            elements.add(BATCH_START_BYTES);
            for (HttpSinkRequestEntry entry : reqeustBatch) {
                elements.add(entry.element);
                elements.add(BATCH_ELEMENT_DELIM_BYTES);
            }
            elements.set(elements.size() - 1, BATCH_END_BYTES);


            byte[] byteArray = elements.stream().collect(
                    ByteArrayOutputStream::new,
                    (b, e) -> b.write(e, 0, e.length),
                    (a, b) -> {
                    }
            ).toByteArray();


            Request.Builder builder = new Request.Builder()
                    .url(endpointUrl)
                    .method(method,
                            RequestBody.create(byteArray, MediaType.parse("application/json")));

            if (!headerMap.isEmpty()) {
                this.headerMap.forEach(builder::addHeader);
                builder.header(HEADER_REQUEST_TIMEOUT_SECONDS, String.valueOf(this.httpRequestTimeOutSeconds));
            }

            return new HttpRequest(builder.build(), elements, method);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
