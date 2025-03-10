package com.getindata.connectors.http.internal.utils;

import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;
import com.getindata.connectors.http.internal.security.SecurityContext;
import com.getindata.connectors.http.internal.security.SelfSignedTrustManager;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.util.StringUtils;
import org.jetbrains.annotations.NotNull;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants.HEADER_REQUEST_TIMEOUT_SECONDS;

@Slf4j
@NoArgsConstructor(access = AccessLevel.NONE)
public class OkHttpClientFactory {

    /**
     * Creates an OkHttpClient instance with default settings.
     *
     * @param properties properties used to build SSLContext
     * @return new OkHttpClient instance.
     */
    public static OkHttpClient createClient(Properties properties) {

        Pair<SSLContext, X509TrustManager> context = getSslContext(properties);

        return new OkHttpClient.Builder()
                .followRedirects(true)
                .sslSocketFactory(context.getLeft().getSocketFactory(), context.getRight())
                .addInterceptor(headerRequestTimeoutInterceptor())
                .build();
    }

    /**
     * Creates an OkHttpClient instance with a custom Executor for async calls.
     *
     * @param properties properties used to build SSLContext
     * @param executor   Executor for async calls.
     * @return new OkHttpClient instance.
     */
    public static OkHttpClient createClient(Properties properties, ExecutorService executor) {

        Pair<SSLContext, X509TrustManager> context = getSslContext(properties);

        return new OkHttpClient.Builder()
                .followRedirects(true)
                .sslSocketFactory(context.getLeft().getSocketFactory(), context.getRight())
                .addInterceptor(headerRequestTimeoutInterceptor())
                .dispatcher(new okhttp3.Dispatcher(executor))
                .build();
    }

    private static Interceptor headerRequestTimeoutInterceptor() {
        return new Interceptor() {
            @NotNull
            @Override
            public Response intercept(@NotNull Chain chain) throws IOException {
                Request request = chain.request();

                String timeout = request.header(HEADER_REQUEST_TIMEOUT_SECONDS);

                if (StringUtils.isNullOrWhitespaceOnly(timeout)) {
                    return chain.proceed(request);
                } else {
                    return chain
                            .withReadTimeout(Integer.parseInt(timeout), TimeUnit.SECONDS)
                            .proceed(request.newBuilder().removeHeader(HEADER_REQUEST_TIMEOUT_SECONDS).build());

                }
            }
        };
    }

    /**
     * Creates an {@link SSLContext} based on provided properties.
     * <ul>
     *     <li>{@link HttpConnectorConfigConstants#ALLOW_SELF_SIGNED}</li>
     *     <li>{@link HttpConnectorConfigConstants#SERVER_TRUSTED_CERT}</li>
     *     <li>{@link HttpConnectorConfigConstants#PROP_DELIM}</li>
     *     <li>{@link HttpConnectorConfigConstants#CLIENT_CERT}</li>
     *     <li>{@link HttpConnectorConfigConstants#CLIENT_PRIVATE_KEY}</li>
     * </ul>
     *
     * @param properties properties used to build {@link SSLContext}
     * @return new {@link SSLContext} instance.
     */
    private static Pair<SSLContext, X509TrustManager> getSslContext(Properties properties) {
        SecurityContext securityContext = createSecurityContext(properties);

        boolean selfSignedCert = Boolean.parseBoolean(
                properties.getProperty(HttpConnectorConfigConstants.ALLOW_SELF_SIGNED, "false"));

        String[] serverTrustedCerts = properties
                .getProperty(HttpConnectorConfigConstants.SERVER_TRUSTED_CERT, "")
                .split(HttpConnectorConfigConstants.PROP_DELIM);

        String clientCert = properties
                .getProperty(HttpConnectorConfigConstants.CLIENT_CERT, "");

        String clientPrivateKey = properties
                .getProperty(HttpConnectorConfigConstants.CLIENT_PRIVATE_KEY, "");

        for (String cert : serverTrustedCerts) {
            if (!StringUtils.isNullOrWhitespaceOnly(cert)) {
                securityContext.addCertToTrustStore(cert);
            }
        }

        if (!StringUtils.isNullOrWhitespaceOnly(clientCert)
                && !StringUtils.isNullOrWhitespaceOnly(clientPrivateKey)) {
            securityContext.addMTlsCerts(clientCert, clientPrivateKey);
        }

        // NOTE TrustManagers must be created AFTER adding all certificates to KeyStore.
        TrustManager[] trustManagers = getTrustedManagers(securityContext, selfSignedCert);
        return Pair.of(securityContext.getSslContext(trustManagers), (X509TrustManager) trustManagers[0]);
    }

    private static TrustManager[] getTrustedManagers(
            SecurityContext securityContext,
            boolean selfSignedCert) {

        TrustManager[] trustManagers = securityContext.getTrustManagers();

        if (selfSignedCert) {
            return wrapWithSelfSignedManagers(trustManagers).toArray(new TrustManager[0]);
        } else {
            return trustManagers;
        }
    }

    private static List<TrustManager> wrapWithSelfSignedManagers(TrustManager[] trustManagers) {
        log.warn("Creating Trust Managers for self-signed certificates - not Recommended. "
                + "Use [" + HttpConnectorConfigConstants.SERVER_TRUSTED_CERT + "] "
                + "connector property to add certificated as trusted.");

        List<TrustManager> selfSignedManagers = new ArrayList<>(trustManagers.length);
        for (TrustManager trustManager : trustManagers) {
            selfSignedManagers.add(new SelfSignedTrustManager((X509TrustManager) trustManager));
        }
        return selfSignedManagers;
    }

    /**
     * Creates a {@link SecurityContext} with empty {@link java.security.KeyStore} or loaded from
     * file.
     *
     * @param properties Properties for creating {@link SecurityContext}
     * @return new {@link SecurityContext} instance.
     */
    private static SecurityContext createSecurityContext(Properties properties) {

        String keyStorePath =
                properties.getProperty(HttpConnectorConfigConstants.KEY_STORE_PATH, "");

        if (StringUtils.isNullOrWhitespaceOnly(keyStorePath)) {
            return SecurityContext.create();
        } else {
            char[] storePassword =
                    properties.getProperty(HttpConnectorConfigConstants.KEY_STORE_PASSWORD, "")
                            .toCharArray();
            if (storePassword.length == 0) {
                throw new RuntimeException("Missing password for provided KeyStore");
            }
            return SecurityContext.createFromKeyStore(keyStorePath, storePassword);
        }
    }

}
