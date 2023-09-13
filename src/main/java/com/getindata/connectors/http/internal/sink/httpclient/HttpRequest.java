package com.getindata.connectors.http.internal.sink.httpclient;

import lombok.Data;
import okhttp3.Request;

import java.util.List;

@Data
public class HttpRequest {

    public final Request request;

    public final List<byte[]> elements;

    public final String method;

}
