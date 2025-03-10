package com.getindata.connectors.http.internal.table.lookup;

import lombok.Data;
import lombok.ToString;
import okhttp3.Request;

/**
 * Wrapper class around {@link okhttp3.Request} that contains information about an actual lookup request
 * body or request parameters.
 */
@Data
@ToString
public class HttpLookupSourceRequestEntry {

    /**
     * Wrapped {@link okhttp3.Request} object.
     */
    private final Request httpRequest;

    /**
     * This field represents lookup query. Depending on used REST request method, this field can
     * represent a request body, for example a Json string when PUT/POST requests method was used,
     * or it can represent a query parameters if GET method was used.
     */
    private final String lookupQuery;

    public HttpLookupSourceRequestEntry(Request httpRequest, String lookupQuery) {

        this.httpRequest = httpRequest;
        this.lookupQuery = lookupQuery;
    }
}
