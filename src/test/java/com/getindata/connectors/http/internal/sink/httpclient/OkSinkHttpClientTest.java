package com.getindata.connectors.http.internal.sink.httpclient;

import com.getindata.connectors.http.HttpPostRequestCallback;
import com.getindata.connectors.http.internal.HeaderPreprocessor;
import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;
import com.getindata.connectors.http.internal.table.sink.Slf4jHttpPostRequestCallback;
import com.getindata.connectors.http.internal.utils.HttpHeaderUtils;
import okhttp3.OkHttpClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Properties;
import java.util.stream.Stream;

import static com.getindata.connectors.http.TestHelper.assertPropertyArray;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OkSinkHttpClientTest {

    private static MockedStatic<OkHttpClient> httpClientStaticMock;

    @Mock
    private OkHttpClient.Builder httpClientBuilder;
    @BeforeAll
    public static void beforeAll() {
        httpClientStaticMock = mockStatic(OkHttpClient.class);
    }

    protected HeaderPreprocessor headerPreprocessor;

    protected HttpPostRequestCallback<HttpRequest> postRequestCallback;

    @AfterAll
    public static void afterAll() {
        if (httpClientStaticMock != null) {
            httpClientStaticMock.close();
        }
    }

    @BeforeEach
    public void setUp() {
        postRequestCallback = new Slf4jHttpPostRequestCallback();
        headerPreprocessor = HttpHeaderUtils.createDefaultHeaderPreprocessor();
        httpClientStaticMock.when(OkHttpClient.Builder::new).thenReturn(httpClientBuilder);
        when(httpClientBuilder.followRedirects(any())).thenReturn(httpClientBuilder);
        when(httpClientBuilder.socketFactory(any())).thenReturn(httpClientBuilder);
        when(httpClientBuilder.dispatcher(any())).thenReturn(httpClientBuilder);
    }

    private static Stream<Arguments> provideSubmitterFactory() {
        return Stream.of(
            Arguments.of(new PerRequestRequestSubmitterFactory()),
            Arguments.of(new BatchRequestSubmitterFactory(50))
        );
    }

    @ParameterizedTest
    @MethodSource("provideSubmitterFactory")
    public void shouldBuildClientWithoutHeaders(RequestSubmitterFactory requestSubmitterFactory) {

        OkSinkHttpClient client =
            new OkSinkHttpClient(
                new Properties(),
                postRequestCallback,
                this.headerPreprocessor,
                requestSubmitterFactory
            );
        assertThat(client.getHeadersAndValues()).isEmpty();
    }

    @ParameterizedTest
    @MethodSource("provideSubmitterFactory")
    public void shouldBuildClientWithHeaders(RequestSubmitterFactory requestSubmitterFactory) {

        // GIVEN
        Properties properties = new Properties();
        properties.setProperty("property", "val1");
        properties.setProperty("my.property", "val2");
        properties.setProperty(
            HttpConnectorConfigConstants.SINK_HEADER_PREFIX + "Origin",
            "https://developer.mozilla.org")
        ;
        properties.setProperty(
            HttpConnectorConfigConstants.SINK_HEADER_PREFIX + "Cache-Control",
            "no-cache, no-store, max-age=0, must-revalidate"
        );
        properties.setProperty(
            HttpConnectorConfigConstants.SINK_HEADER_PREFIX + "Access-Control-Allow-Origin",
            "*"
        );

        // WHEN
        OkSinkHttpClient client = new OkSinkHttpClient(
            properties,
            postRequestCallback,
            headerPreprocessor,
            requestSubmitterFactory
        );
        String[] headersAndValues = client.getHeadersAndValues();
        assertThat(headersAndValues).hasSize(6);

        // THEN
        // assert that we have property followed by its value.
        assertPropertyArray(headersAndValues, "Origin", "https://developer.mozilla.org");
        assertPropertyArray(
            headersAndValues,
            "Cache-Control", "no-cache, no-store, max-age=0, must-revalidate"
        );
        assertPropertyArray(headersAndValues, "Access-Control-Allow-Origin", "*");
    }

}
