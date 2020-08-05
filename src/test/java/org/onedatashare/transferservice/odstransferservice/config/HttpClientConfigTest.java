package org.onedatashare.transferservice.odstransferservice.config;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestFactory;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.onedatashare.transferservice.odstransferservice.model.EndpointCredential;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;



@RunWith(SpringJUnit4ClassRunner.class)
public class HttpClientConfigTest {

    HttpClientConfig config = new HttpClientConfig();

    @Test
    public void loadConfigTestNotNull(){
        assertNotNull(config.poolingHttpClientConnectionManager());
    }

    @Test
    public void maxConnectionsIsTwentyTest(){
        config.setMaxConnections(10);
        assertEquals(10, config.poolingHttpClientConnectionManager().getMaxTotal());
    }


    @Test
    public void httpCloseableConnectionNullTest(){
        assertNotNull(config.httpClient());
    }

/*    @Test
    public void idleConnectionMonitor() throws Exception {
        CloseableHttpClient client = config.httpClient();
        HttpGet get = new HttpGet("localhost/");
        EndpointCredential cred = mock(new EndpointCredential);
        CloseableHttpResponse response = client.execute(get);

    }
 */
}
