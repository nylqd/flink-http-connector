package com.getindata.connectors.http.internal.table.lookup;

import com.getindata.connectors.http.LookupQueryCreator;
import com.getindata.connectors.http.internal.HeaderPreprocessor;
import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;
import com.getindata.connectors.http.internal.utils.HttpHeaderUtils;
import okhttp3.Request;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;

import java.util.Map;

/**
 * Base class for {@link okhttp3.Request} factories.
 */
public abstract class RequestFactoryBase implements HttpRequestFactory {

    public static final String DEFAULT_REQUEST_TIMEOUT_SECONDS = "30";

    /**
     * Base url used for {@link okhttp3.Request} for example "http://localhost:8080"
     */
    protected final String baseUrl;

    protected final LookupQueryCreator lookupQueryCreator;

    protected final int httpRequestTimeOutSeconds;

    /**
     * HTTP headers that should be used for {@link okhttp3.Request} created by factory.
     */
    private final Map<String, String> headerMap;

    public RequestFactoryBase(
            LookupQueryCreator lookupQueryCreator,
            HeaderPreprocessor headerPreprocessor,
            HttpLookupConfig options) {

        this.baseUrl = options.getUrl();
        this.lookupQueryCreator = lookupQueryCreator;

        this.headerMap = HttpHeaderUtils
            .prepareHeaderMap(
                HttpConnectorConfigConstants.LOOKUP_SOURCE_HEADER_PREFIX,
                options.getProperties(),
                headerPreprocessor
            );

        this.httpRequestTimeOutSeconds = Integer.parseInt(
            options.getProperties().getProperty(
                HttpConnectorConfigConstants.LOOKUP_HTTP_TIMEOUT_SECONDS,
                DEFAULT_REQUEST_TIMEOUT_SECONDS
            )
        );
    }

    @Override
    public HttpLookupSourceRequestEntry buildLookupRequest(RowData lookupRow) {

        String lookupQuery = lookupQueryCreator.createLookupQuery(lookupRow);
        getLogger().debug("Created Http lookup query: " + lookupQuery);

        Request.Builder requestBuilder = setUpRequestMethod(lookupQuery);

        if (!headerMap.isEmpty()) {
            headerMap.forEach(requestBuilder::addHeader);
        }

        return new HttpLookupSourceRequestEntry(requestBuilder.build(), lookupQuery);
    }

    protected abstract Logger getLogger();

    /**
     * Method for preparing {@link okhttp3.Request.Builder} for concrete REST method.
     * @param lookupQuery lookup query used for request query parameters or body.
     * @return {@link okhttp3.Request.Builder} for given lookupQuery.
     */
    protected abstract Request.Builder setUpRequestMethod(String lookupQuery);

    @VisibleForTesting
    Map<String, String> getHeaderMap() {
        return headerMap;
    }
}
