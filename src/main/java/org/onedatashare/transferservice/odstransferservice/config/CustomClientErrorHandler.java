package org.onedatashare.transferservice.odstransferservice.config;

import org.springframework.http.client.ClientHttpResponse;
import org.springframework.web.client.ResponseErrorHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CustomClientErrorHandler implements ResponseErrorHandler {
    private final Logger LOG = LoggerFactory.getLogger(CustomClientErrorHandler.class);

    @Override
    public boolean hasError(ClientHttpResponse response) throws IOException {
        return response.getStatusCode().is4xxClientError();
    }

    @Override
    public void handleError(ClientHttpResponse response) throws IOException {
        LOG.error("CustomClientErrorHandler | HTTP Status Code: " + response.getStatusCode().value());
    }
}
