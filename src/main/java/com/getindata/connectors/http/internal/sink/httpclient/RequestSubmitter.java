package com.getindata.connectors.http.internal.sink.httpclient;

import com.getindata.connectors.http.internal.sink.HttpSinkRequestEntry;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Submits request via HTTP.
 */
public interface RequestSubmitter {

    List<CompletableFuture<OkHttpResponseWrapper>> submit(
        String endpointUrl,
        List<HttpSinkRequestEntry> requestToSubmit);
}
