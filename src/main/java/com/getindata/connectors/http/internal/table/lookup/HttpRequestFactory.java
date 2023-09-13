package com.getindata.connectors.http.internal.table.lookup;

import org.apache.flink.table.data.RowData;

import java.io.Serializable;

/**
 * Factory for creating {@link okhttp3.Request} objects for Rest clients.
 */
public interface HttpRequestFactory extends Serializable {

    /**
     * Creates a {@link okhttp3.Request} from given {@link RowData}.
     *
     * @param lookupRow {@link RowData} object used for building http request.
     * @return {@link okhttp3.Request} created from {@link RowData}
     */
    HttpLookupSourceRequestEntry buildLookupRequest(RowData lookupRow);
}
