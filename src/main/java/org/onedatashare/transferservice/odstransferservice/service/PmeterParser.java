package org.onedatashare.transferservice.odstransferservice.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import jakarta.annotation.PostConstruct;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.PumpStreamHandler;
import org.onedatashare.transferservice.odstransferservice.model.CarbonIpEntry;
import org.onedatashare.transferservice.odstransferservice.model.metrics.CarbonScore;
import org.onedatashare.transferservice.odstransferservice.model.metrics.DataInflux;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

@Service
public class PmeterParser {

    private final String MEASURE = "measure";
    private final ByteArrayOutputStream outputStream;
    private final PumpStreamHandler streamHandler;
    private final DefaultExecutor pmeterExecutor;
    private final ExecuteWatchdog watchDog;

    @Value("${pmeter.nic}")
    private String pmeterNic;

    Logger logger = LoggerFactory.getLogger(PmeterParser.class);

    @Value("${pmeter.carbon.path}")
    String pmeterCarbonPath;

    @Value("${pmeter.carbon.map}")
    String pmeterCarbonMapPath;

    @Value("${pmeter.report.path}")
    String pmeterMetricsPath;

    @Value("${ods.user}")
    String odsUser;

    @Value("${pmeter.measure}")
    int measureCount;

    @Value("${pmeter.options}")
    String pmeterOptions;

    @Value("${pmeter.carbon.toggle}")
    private boolean toggle;

    ObjectMapper pmeterMapper;
    private CommandLine cmdLine;

    @PostConstruct
    public void init() throws IOException {
        if(this.pmeterNic == null || !this.pmeterNic.isEmpty()) {
            this.pmeterNic = this.discoverActiveNetworkInterface();
        }
        logger.info("Interface used for monitoring: {}", this.pmeterNic);
        this.cmdLine = CommandLine.parse(String.format("pmeter " + MEASURE + " %s --user %s --measure %s %s --file_name %s", this.pmeterNic, odsUser, measureCount, pmeterOptions, pmeterMetricsPath));
    }

    public PmeterParser() {
        this.outputStream = new ByteArrayOutputStream();
        this.streamHandler = new PumpStreamHandler(outputStream);

        this.pmeterExecutor = new DefaultExecutor();
        this.watchDog = new ExecuteWatchdog(ExecuteWatchdog.INFINITE_TIMEOUT);
        pmeterExecutor.setWatchdog(watchDog);
        pmeterExecutor.setStreamHandler(streamHandler);

        this.pmeterMapper = new ObjectMapper();
        this.pmeterMapper.registerModule(new JavaTimeModule());
        this.pmeterMapper.configure(SerializationFeature.WRITE_DATE_KEYS_AS_TIMESTAMPS, false);

    }


    public void runPmeter() {
        try {
            pmeterExecutor.execute(cmdLine);
        } catch (IOException e) {
            logger.error("Failed in executing pmeter script:\n " + cmdLine);
            e.printStackTrace();
        }
    }

    public List<DataInflux> parsePmeterOutput() throws IOException {
        Path path = Paths.get(pmeterMetricsPath);
        List<String> allLines = Files.readAllLines(path);
        List<DataInflux> ret = new ArrayList<>();
        for (String line : allLines) {
            DataInflux dataInflux = this.pmeterMapper.readValue(line, DataInflux.class);
            ret.add(dataInflux);
        }
        path.toFile().delete();
        path.toFile().createNewFile();
        return ret;
    }

    public CarbonScore carbonAverageTraceRoute(String ip) {
        //pmeter carbon 129.114.108.45
        if (this.toggle == false || ip == null || ip.isEmpty()) return new CarbonScore();

        CommandLine carbonCmd = CommandLine.parse(String.format("pmeter carbon %s", ip));
        try {
            DefaultExecutor carbonExecutor = new DefaultExecutor();
            carbonExecutor.execute(carbonCmd);
            Path filePath = Paths.get(this.pmeterCarbonPath);
            List<String> lines = Files.readAllLines(filePath);
            CarbonScore score = new CarbonScore();
            for (String line : lines) {
                score = this.pmeterMapper.readValue(line, CarbonScore.class);
                break;
            }
            filePath.toFile().delete();
            filePath.toFile().createNewFile();
            return score;
        } catch (IOException e) {
            logger.error("Error Carbon Average Trace Route:\n {}", e.getMessage());
            return new CarbonScore();
        }
    }

    public List<CarbonIpEntry> carbonPerIp(String ip) throws IOException {
        if (ip == null || ip.isEmpty()) return new ArrayList<>();
        CommandLine carbonCmd = CommandLine.parse(String.format("pmeter carbon %s --save_per_ip=True", ip));
        DefaultExecutor carbonExecutor = new DefaultExecutor();
        carbonExecutor.execute(carbonCmd);
        Path filePath = Paths.get(this.pmeterCarbonMapPath);
        List<String> lines = Files.readAllLines(filePath);
        String lastLine = lines.getLast();
        Map<String, Object> measurement = this.pmeterMapper.readValue(lastLine, new TypeReference<Map<String, Object>>() {
        });
        List<CarbonIpEntry> retList = new ArrayList<>();
        for (Map.Entry<String, Object> entry : measurement.entrySet()) {
            if (entry.getKey().equals("time")) continue;
            LinkedHashMap<String, Object> value = (LinkedHashMap<String, Object>) entry.getValue();
            CarbonIpEntry carbonIpEntry = new CarbonIpEntry();
            carbonIpEntry.setIp(entry.getKey());
            carbonIpEntry.setCarbonIntensity((int) value.get("carbon_intensity"));
            carbonIpEntry.setLon((double) value.get("lon"));
            carbonIpEntry.setLat((double) value.get("lat"));
            retList.add(carbonIpEntry);
        }
        filePath.toFile().delete();
        filePath.toFile().createNewFile();
        return retList;
    }

    public String discoverActiveNetworkInterface() throws IOException {
        // iterate over the network interfaces known to java
        Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
        for (NetworkInterface interface_ : Collections.list(interfaces)) {
            // we shouldn't care about loopback addresses
            if (interface_.isLoopback())
                continue;

            // if you don't expect the interface to be up you can skip this
            // though it would question the usability of the rest of the code
            if (!interface_.isUp())
                continue;

            // iterate over the addresses associated with the interface
            Enumeration<InetAddress> addresses = interface_.getInetAddresses();
            for (InetAddress address : Collections.list(addresses)) {
                // look only for ipv4 addresses
                logger.info(address.getHostAddress());
                if (address instanceof Inet6Address)
                    continue;

                // use a timeout big enough for your needs
                if (!address.isReachable(3000))
                    continue;

                // java 7's try-with-resources statement, so that
                // we close the socket immediately after use
                try (SocketChannel socket = SocketChannel.open()) {
                    // again, use a big enough timeout
                    socket.socket().setSoTimeout(3000);

                    // bind the socket to your local interface
                    socket.bind(new InetSocketAddress(address, 8080));

                    // try to connect to *somewhere*
                    socket.connect(new InetSocketAddress("onedatashare.org", 80));
                } catch (IOException ex) {
                    continue;
                }
                logger.info("Interface used for Transfer-Service: {}", interface_.getDisplayName());
                return interface_.getDisplayName();
            }
        }
        return "";
    }
}
