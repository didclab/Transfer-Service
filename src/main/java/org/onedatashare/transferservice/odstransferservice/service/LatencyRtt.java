package org.onedatashare.transferservice.odstransferservice.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.time.LocalDateTime;

/**
 * A service that measures the RTT to a server over a TCP socket.
 */
@Service
public class LatencyRtt {

    Logger logger = LoggerFactory.getLogger(LatencyRtt.class);

    /**
     * @param host: ip or host name
     * @param port: port number to use when opening connection
     * @return
     * @throws IOException
     */
    public double rttCompute(String host, int port) throws IOException {
        return this.rttCompute(host, port, 0);
    }

    /**
     * @param host
     * @param port
     * @param timeout
     * @return
     * @throws IOException
     */
    public double rttCompute(String host, int port, int timeout) throws IOException {
        if (host.isEmpty() || port < 1) {
            return 0.0;
        }
        LocalDateTime startTime = LocalDateTime.now();
        try (Socket soc = new Socket()) {
            logger.info("Trying to connect using a socket Host:{}, port:{}", host, port);
            soc.connect(new InetSocketAddress(host, port), timeout);
            LocalDateTime endTime = LocalDateTime.now();
            logger.info("RTT: {}, Host: {}, Port: {}", Duration.between(startTime, endTime).toMillis(), host, port);
            return (double) Duration.between(startTime, endTime).toMillis();
        }
    }
}
