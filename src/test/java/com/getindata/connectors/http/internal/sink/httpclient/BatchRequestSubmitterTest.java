package com.getindata.connectors.http.internal.sink.httpclient;

import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;
import com.getindata.connectors.http.internal.sink.HttpSinkRequestEntry;
import okhttp3.OkHttpClient;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class BatchRequestSubmitterTest {

    @Mock
    private OkHttpClient mockHttpClient;

    @ParameterizedTest
    @CsvSource(value = {"50, 1", "5, 1", "3, 2", "2, 3", "1, 5"})
    public void submitBatches(int batchSize, int expectedNumberOfBatchRequests) {

        Properties properties = new Properties();
        properties.setProperty(
            HttpConnectorConfigConstants.SINK_HTTP_BATCH_REQUEST_SIZE,
            String.valueOf(batchSize)
        );

        BatchRequestSubmitter submitter = new BatchRequestSubmitter(
            properties,
            new HashMap<>(),
            mockHttpClient
        );

        submitter.submit(
            "http://hello.pl",
            IntStream.range(0, 5)
                .mapToObj(val -> new HttpSinkRequestEntry("PUT", new byte[0]))
                .collect(Collectors.toList())
        );

        verify(mockHttpClient, times(expectedNumberOfBatchRequests)).newCall(any()).enqueue(any());
    }

    private static Stream<Arguments> httpRequestMethods() {
        return Stream.of(
            Arguments.of(Arrays.asList("PUT", "PUT", "PUT", "PUT", "POST"), 2),
            Arguments.of(Arrays.asList("PUT", "PUT", "PUT", "POST", "PUT"), 3),
            Arguments.of(Arrays.asList("POST", "PUT", "POST", "POST", "PUT"), 4)
        );
    }
    @ParameterizedTest
    @MethodSource("httpRequestMethods")
    public void shouldSplitBatchPerHttpMethod(
            List<String> httpMethods,
            int expectedNumberOfBatchRequests) {

        Properties properties = new Properties();
        properties.setProperty(
            HttpConnectorConfigConstants.SINK_HTTP_BATCH_REQUEST_SIZE,
            String.valueOf(50)
        );


        BatchRequestSubmitter submitter = new BatchRequestSubmitter(
            properties,
            new HashMap<>(),
            mockHttpClient
        );

        submitter.submit(
            "http://hello.pl",
            httpMethods.stream()
                .map(method -> new HttpSinkRequestEntry(method, new byte[0]))
                .collect(Collectors.toList())
        );

        verify(mockHttpClient, times(expectedNumberOfBatchRequests)).newCall(any()).enqueue(any());
    }
}
