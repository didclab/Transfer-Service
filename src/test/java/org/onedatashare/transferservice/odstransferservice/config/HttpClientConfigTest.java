//package org.onedatashare.transferservice.odstransferservice.config;
//
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
//
//import static org.mockito.Mockito.mock;
//import static org.junit.jupiter.api.Assertions.assertEquals;
//import static org.junit.jupiter.api.Assertions.assertNotNull;
//
//
//
//@RunWith(SpringJUnit4ClassRunner.class)
//public class HttpClientConfigTest {
//
//    HttpClientConfig config = new HttpClientConfig();
//
//    @Test
//    public void loadConfigTestNotNull(){
//        assertNotNull(config.poolingHttpClientConnectionManager());
//    }
//
//    @Test
//    public void maxConnectionsIsTwentyTest(){
//        config.setMaxConnections(10);
//        assertEquals(10, config.poolingHttpClientConnectionManager().getMaxTotal());
//    }
//
//
//    @Test
//    public void httpCloseableConnectionNullTest(){
//        assertNotNull(config.httpClient());
//    }
//
///*    @Test
//    public void idleConnectionMonitor() throws Exception {
//        CloseableHttpClient client = config.httpClient();
//        HttpGet get = new HttpGet("localhost/");
//        EndpointCredential cred = mock(new EndpointCredential);
//        CloseableHttpResponse response = client.execute(get);
//
//    }
// */
//}
