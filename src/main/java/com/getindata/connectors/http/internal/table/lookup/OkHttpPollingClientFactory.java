package com.getindata.connectors.http.internal.table.lookup;

import com.getindata.connectors.http.internal.PollingClientFactory;
import com.getindata.connectors.http.internal.utils.OkHttpClientFactory;
import okhttp3.OkHttpClient;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.data.RowData;

public class OkHttpPollingClientFactory implements PollingClientFactory<RowData> {

    private final HttpRequestFactory requestFactory;

    public OkHttpPollingClientFactory(HttpRequestFactory requestFactory) {
        this.requestFactory = requestFactory;
    }

    @Override
    public OkHttpPollingClient createPollClient(
            HttpLookupConfig options,
            DeserializationSchema<RowData> schemaDecoder) {

        OkHttpClient okHttpClient = OkHttpClientFactory.createClient(options.getProperties());

        return new OkHttpPollingClient(
            okHttpClient,
            schemaDecoder,
            options,
            requestFactory
        );
    }
}
