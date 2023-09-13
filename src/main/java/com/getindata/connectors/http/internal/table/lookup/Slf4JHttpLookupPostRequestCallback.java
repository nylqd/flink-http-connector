package com.getindata.connectors.http.internal.table.lookup;

import com.getindata.connectors.http.HttpPostRequestCallback;
import com.getindata.connectors.http.internal.utils.ConfigUtils;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;
import java.util.Map;
import java.util.StringJoiner;

/**
 * A {@link HttpPostRequestCallback} that logs pairs of request and response as <i>INFO</i> level
 * logs using <i>Slf4j</i>.
 *
 * <p>Serving as a default implementation of {@link HttpPostRequestCallback} for
 * the {@link HttpLookupTableSource}.
 */
@Slf4j
public class Slf4JHttpLookupPostRequestCallback
        implements HttpPostRequestCallback<HttpLookupSourceRequestEntry> {

    @Override
    public void call(
            Response response,
            HttpLookupSourceRequestEntry requestEntry,
            String endpointUrl,
            Map<String, String> headerMap) {

        Request request = requestEntry.getHttpRequest();
        StringJoiner headers = new StringJoiner(";");

        request.headers().forEach(header -> headers.add(header.component1() +":[" +header.component2() + "]"));


        if (response == null) {
            log.info(
                "Got response for a request.\n  Request:\n    URL: {}\n    " +
                    "Method: {}\n    Headers: {}\n    Params/Body: {}\n  Response: null",
                request.url(),
                request.method(),
                headers,
                requestEntry.getLookupQuery()
            );
        } else {
            try {
                log.info(
                    "Got response for a request.\n  Request:\n    URL: {}\n    " +
                        "Method: {}\n    Headers: {}\n    Params/Body: {}\n  Response: {}\n    Body: {}",
                    request.url(),
                    request.method(),
                    headers,
                    requestEntry.getLookupQuery(),
                    response,
                    response.body().string().replaceAll(ConfigUtils.UNIVERSAL_NEW_LINE_REGEXP, "")
                );
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }
}
