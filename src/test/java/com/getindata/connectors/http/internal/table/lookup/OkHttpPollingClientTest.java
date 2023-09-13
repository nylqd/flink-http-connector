package com.getindata.connectors.http.internal.table.lookup;

import com.getindata.connectors.http.internal.HeaderPreprocessor;
import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;
import com.getindata.connectors.http.internal.table.lookup.querycreators.GenericGetQueryCreator;
import com.getindata.connectors.http.internal.utils.HttpHeaderUtils;
import okhttp3.OkHttpClient;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
public class OkHttpPollingClientTest {

    @Mock
    private OkHttpClient httpClient;

    @Mock
    private DeserializationSchema<RowData> decoder;

    @Mock
    private LookupRow lookupRow;

    private HeaderPreprocessor headerPreprocessor;

    private HttpLookupConfig options;

    @BeforeEach
    public void setUp() {
        this.headerPreprocessor = HttpHeaderUtils.createDefaultHeaderPreprocessor();
        this.options = HttpLookupConfig.builder().build();
    }

    @Test
    public void shouldBuildClientWithoutHeaders() {

        OkHttpPollingClient client = new OkHttpPollingClient(
            httpClient,
            decoder,
            options,
            new GetRequestFactory(
                new GenericGetQueryCreator(lookupRow),
                headerPreprocessor,
                options
            )
        );

        assertThat(
            ((GetRequestFactory) client.getRequestFactory()).getHeaderMap())
            .isEmpty();
    }

    @Test
    public void shouldBuildClientWithHeaders() {

        // GIVEN
        Properties properties = new Properties();
        properties.setProperty("property", "val1");
        properties.setProperty("my.property", "val2");
        properties.setProperty(
            HttpConnectorConfigConstants.LOOKUP_SOURCE_HEADER_PREFIX + "Origin",
            "https://developer.mozilla.org");

        properties.setProperty(
            HttpConnectorConfigConstants.LOOKUP_SOURCE_HEADER_PREFIX + "Cache-Control",
            "no-cache, no-store, max-age=0, must-revalidate"
        );
        properties.setProperty(
            HttpConnectorConfigConstants.LOOKUP_SOURCE_HEADER_PREFIX
                + "Access-Control-Allow-Origin", "*"
        );

        // WHEN
        HttpLookupConfig lookupConfig = HttpLookupConfig.builder()
            .properties(properties)
            .build();

        OkHttpPollingClient client = new OkHttpPollingClient(
            httpClient,
            decoder,
            lookupConfig,
            new GetRequestFactory(
                new GenericGetQueryCreator(lookupRow),
                headerPreprocessor,
                lookupConfig
            )
        );


        Map<String, String> headerMap = ((GetRequestFactory) client.getRequestFactory()).getHeaderMap();
        assertThat(headerMap).hasSize(3);

        // THEN
        // assert that we have property followed by its value.
        assertThat(headerMap.get("Origin")).isEqualTo("https://developer.mozilla.org");
        assertThat(headerMap.get("Cache-Control")).isEqualTo("no-cache, no-store, max-age=0, must-revalidate");
        assertThat(headerMap.get("Access-Control-Allow-Origin")).isEqualTo("*");
    }
}
