package com.getindata.connectors.http.internal.table.lookup;

import com.getindata.connectors.http.LookupQueryCreator;
import com.getindata.connectors.http.internal.HeaderPreprocessor;
import com.getindata.connectors.http.internal.utils.uri.URIBuilder;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Request;
import org.slf4j.Logger;

import java.net.URI;
import java.net.URISyntaxException;

import static com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants.HEADER_REQUEST_TIMEOUT_SECONDS;

/**
 * Implementation of {@link HttpRequestFactory} for GET REST calls.
 */
@Slf4j
public class GetRequestFactory extends RequestFactoryBase {

    public GetRequestFactory(
            LookupQueryCreator lookupQueryCreator,
            HeaderPreprocessor headerPreprocessor,
            HttpLookupConfig options) {

        super(lookupQueryCreator, headerPreprocessor, options);
    }

    @Override
    protected Logger getLogger() {
        return log;
    }

    /**
     * Method for preparing {@link Request.Builder} for REST GET request, where lookupQuery
     * is used as query parameters for example:
     * <pre>
     *     http:localhost:8080/service?id=1
     * </pre>
     * @param lookupQuery lookup query used for request query parameters.
     * @return {@link Request.Builder} for given GET lookupQuery
     */
    @Override
    protected Request.Builder setUpRequestMethod(String lookupQuery) {

        return new Request.Builder()
                .url(constructGetUri(lookupQuery).toString())
                .header(HEADER_REQUEST_TIMEOUT_SECONDS, String.valueOf(this.httpRequestTimeOutSeconds))
                .get();
    }

    private URI constructGetUri(String lookupQuery) {
        try {
            return new URIBuilder(baseUrl + "?" + lookupQuery).build();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
