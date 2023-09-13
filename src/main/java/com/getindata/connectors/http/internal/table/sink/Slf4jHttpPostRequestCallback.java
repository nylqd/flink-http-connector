package com.getindata.connectors.http.internal.table.sink;

import com.getindata.connectors.http.HttpPostRequestCallback;
import com.getindata.connectors.http.internal.sink.httpclient.HttpRequest;
import com.getindata.connectors.http.internal.utils.ConfigUtils;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Response;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A {@link HttpPostRequestCallback} that logs pairs of request and response as <i>INFO</i> level
 * logs using <i>Slf4j</i>.
 *
 * <p>Serving as a default implementation of {@link HttpPostRequestCallback} for
 * the {@link HttpDynamicSink}.
 */
@Slf4j
public class Slf4jHttpPostRequestCallback implements HttpPostRequestCallback<HttpRequest> {

    @Override
    public void call(
        Response response,
        HttpRequest requestEntry,
        String endpointUrl,
        Map<String, String> headerMap) {

        String requestBody = requestEntry.getElements().stream()
            .map(element -> new String(element, StandardCharsets.UTF_8))
            .collect(Collectors.joining());

        if (response == null) {
            log.info(
                "Got response for a request.\n  Request:\n    " +
                "Method: {}\n    Body: {}\n  Response: null",
                requestEntry.getMethod(),
                requestBody
            );
        } else {
            try {
                log.info(
                    "Got response for a request.\n  Request:\n    " +
                    "Method: {}\n    Body: {}\n  Response: {}\n    Body: {}",
                    requestEntry.method,
                    requestBody,
                    response,
                    response.body().string().replaceAll(ConfigUtils.UNIVERSAL_NEW_LINE_REGEXP, "")
                );
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
