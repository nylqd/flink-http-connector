package com.getindata.connectors.http.internal.table.lookup;

import com.getindata.connectors.http.LookupQueryCreator;
import com.getindata.connectors.http.internal.HeaderPreprocessor;
import com.getindata.connectors.http.internal.utils.uri.URIBuilder;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Request;
import okhttp3.RequestBody;
import org.slf4j.Logger;

import java.net.URI;
import java.net.URISyntaxException;

import static com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants.HEADER_REQUEST_TIMEOUT_SECONDS;

/**
 * Implementation of {@link HttpRequestFactory} for REST calls that sends their parameters using
 * request body.
 */
@Slf4j
public class BodyBasedRequestFactory extends RequestFactoryBase {

    private final String methodName;

    public BodyBasedRequestFactory(
            String methodName,
            LookupQueryCreator lookupQueryCreator,
            HeaderPreprocessor headerPreprocessor,
            HttpLookupConfig options) {

        super(lookupQueryCreator, headerPreprocessor, options);
        this.methodName = methodName.toUpperCase();
    }

    /**
     * Method for preparing {@link Request.Builder} for REST request that sends their parameters
     * in request body, for example PUT or POST methods
     *
     * @param lookupQuery lookup query used for request body.
     * @return {@link Request.Builder} for given lookupQuery.
     */
    @Override
    protected Request.Builder setUpRequestMethod(String lookupQuery) {

        return new Request.Builder()
                .url(constructGetUri().toString())
                .header(HEADER_REQUEST_TIMEOUT_SECONDS, String.valueOf(this.httpRequestTimeOutSeconds))
                .method(methodName, RequestBody.create(lookupQuery.getBytes()));
    }

    @Override
    protected Logger getLogger() {
        return log;
    }

    private URI constructGetUri() {
        try {
            return new URIBuilder(baseUrl).build();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
