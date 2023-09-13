package com.getindata.connectors.http.internal.sink.httpclient;

import java.util.Map;
import java.util.Properties;

public interface RequestSubmitterFactory {

    RequestSubmitter createSubmitter(Properties properties, Map<String, String> headerMap);
}
