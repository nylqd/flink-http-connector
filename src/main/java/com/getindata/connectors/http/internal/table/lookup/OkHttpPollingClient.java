package com.getindata.connectors.http.internal.table.lookup;

import com.getindata.connectors.http.HttpPostRequestCallback;
import com.getindata.connectors.http.internal.PollingClient;
import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;
import com.getindata.connectors.http.internal.status.ComposeHttpStatusCodeChecker;
import com.getindata.connectors.http.internal.status.ComposeHttpStatusCodeChecker.ComposeHttpStatusCodeCheckerConfig;
import com.getindata.connectors.http.internal.status.HttpStatusCodeChecker;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.StringUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;

/**
 * An implementation of {@link PollingClient} that uses Java 11's {@link OkHttpClient}.
 * This implementation supports HTTP traffic only.
 */
@Slf4j
public class OkHttpPollingClient implements PollingClient<RowData> {

    private final OkHttpClient okHttpClient;

    private final HttpStatusCodeChecker statusCodeChecker;

    private final DeserializationSchema<RowData> responseBodyDecoder;

    private final HttpRequestFactory requestFactory;

    private final HttpPostRequestCallback<HttpLookupSourceRequestEntry> httpPostRequestCallback;

    public OkHttpPollingClient(
            OkHttpClient okHttpClient,
            DeserializationSchema<RowData> responseBodyDecoder,
            HttpLookupConfig options,
            HttpRequestFactory requestFactory) {

        this.okHttpClient = okHttpClient;
        this.responseBodyDecoder = responseBodyDecoder;
        this.requestFactory = requestFactory;

        // TODO inject same way as it is done for Sink
        this.httpPostRequestCallback = new Slf4JHttpLookupPostRequestCallback();

        // TODO Inject this via constructor when implementing a response processor.
        //  Processor will be injected and it will wrap statusChecker implementation.
        ComposeHttpStatusCodeCheckerConfig checkerConfig =
            ComposeHttpStatusCodeCheckerConfig.builder()
                .properties(options.getProperties())
                .whiteListPrefix(
                    HttpConnectorConfigConstants.HTTP_ERROR_SOURCE_LOOKUP_CODE_WHITE_LIST
                )
                .errorCodePrefix(HttpConnectorConfigConstants.HTTP_ERROR_SOURCE_LOOKUP_CODES_LIST)
                .build();

        this.statusCodeChecker = new ComposeHttpStatusCodeChecker(checkerConfig);
    }

    @Override
    public Optional<RowData> pull(RowData lookupRow) {
        try {
            return queryAndProcess(lookupRow);
        } catch (Exception e) {
            log.error("Exception during HTTP request.", e);
            return Optional.empty();
        }
    }

    // TODO Add Retry Policy And configure TimeOut from properties
    private Optional<RowData> queryAndProcess(RowData lookupData) throws Exception {

        HttpLookupSourceRequestEntry request = requestFactory.buildLookupRequest(lookupData);
        Response response = okHttpClient.newCall(request.getHttpRequest()).execute();

        return processHttpResponse(response, request);
    }

    private Optional<RowData> processHttpResponse(
            Response response,
            HttpLookupSourceRequestEntry request) throws IOException {

        ResponseBody body = response.peekBody(Long.MAX_VALUE);

        this.httpPostRequestCallback.call(response, request, "endpoint", Collections.emptyMap());

        if (response == null) {
            log.warn("Null Http response for request " + request.getHttpRequest().url().toString());
            return Optional.empty();
        }

        String responseBody = body.string();
        int statusCode = response.code();

        log.debug("Received {} status code for RestTableSource Request", statusCode);
        if (notErrorCodeAndNotEmptyBody(responseBody, statusCode)) {
            log.trace("Server response body" + responseBody);
            return Optional.ofNullable(responseBodyDecoder.deserialize(responseBody.getBytes()));
        } else {
            log.warn(
                String.format("Returned Http status code was invalid or returned body was empty. "
                + "Status Code [%s], "
                + "response body [%s]", statusCode, responseBody)
            );

            return Optional.empty();
        }
    }

    private boolean notErrorCodeAndNotEmptyBody(String body, int statusCode) {
        return !(StringUtils.isNullOrWhitespaceOnly(body) || statusCodeChecker.isErrorCode(
            statusCode));
    }

    @VisibleForTesting
    HttpRequestFactory getRequestFactory() {
        return this.requestFactory;
    }
}
