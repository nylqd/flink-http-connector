package com.getindata.connectors.http.internal.sink.httpclient;

import com.getindata.connectors.http.internal.sink.HttpSinkRequestEntry;
import lombok.extern.slf4j.Slf4j;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;

import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * This implementation creates HTTP requests for every processed event.
 */
@Slf4j
public class PerRequestSubmitter extends AbstractRequestSubmitter {

    public PerRequestSubmitter(
            Properties properties,
            Map<String, String> headerMap,
            OkHttpClient okHttpClient) {

        super(properties, headerMap, okHttpClient);
    }

    @Override
    public List<CompletableFuture<OkHttpResponseWrapper>> submit(
            String endpointUrl,
            List<HttpSinkRequestEntry> requestToSubmit) {

        ArrayList<CompletableFuture<OkHttpResponseWrapper>> responseFutures = new ArrayList<>();

        for (HttpSinkRequestEntry httpSinkRequestEntry : requestToSubmit) {
            HttpRequest httpRequest = buildHttpRequest(httpSinkRequestEntry, endpointUrl);

            OkHttpResponseFuture callback = new OkHttpResponseFuture();
            okHttpClient.newCall(httpRequest.getRequest()).enqueue(callback);

            CompletableFuture<OkHttpResponseWrapper> response = callback.future
                    .exceptionally(ex -> {
                        // TODO This will be executed on a ForkJoinPool Thread... refactor this someday.
                        log.error("Request fatally failed because of an exception", ex);
                        return null;
                    })
                    .thenApplyAsync(
                            res -> new OkHttpResponseWrapper(httpRequest, res),
                            publishingThreadPool
                    );

            responseFutures.add(response);
        }
        return responseFutures;
    }

    private HttpRequest buildHttpRequest(HttpSinkRequestEntry requestEntry, String endpointUrl) {

        Request.Builder builder = new Request.Builder()
                .url(endpointUrl)
                .method(requestEntry.method,
                        RequestBody.create(requestEntry.element, MediaType.parse("application/json")));

        if (!headerMap.isEmpty()) {
            this.headerMap.forEach(builder::addHeader);
        }

        return new HttpRequest(
                builder.build(),
                Collections.singletonList(requestEntry.element),
                requestEntry.method
        );
    }
}
