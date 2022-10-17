package org.onedatashare.transferservice.odstransferservice.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.influx.InfluxConfig;
import io.micrometer.influx.InfluxMeterRegistry;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.PumpStreamHandler;
import org.jetbrains.annotations.NotNull;
import org.onedatashare.transferservice.odstransferservice.constant.DataInfluxConstants;
import org.onedatashare.transferservice.odstransferservice.model.metrics.DataInflux;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

@Service
public class PmeterParser {

    private final String MEASURE = "measure";

    Logger logger = LoggerFactory.getLogger(PmeterParser.class);

    @Autowired
    ObjectMapper pmeterMapper;

//    @Value("${pmeter.home}")
//    String pmeterHome;

    @Value("${pmeter.report.path}")
    String pmeterReportPath;

    @Value("${pmeter.interface}")
    String pmeterNic;

    @Value("${ods.user}")
    String odsUser;

    @Value("${pmeter.measure}")
    int measureCount;

    @Value("${pmeter.options}")
    String pmeterOptions;

    private CommandLine cmdLine;

    @PostConstruct
    public void postConstruct(){
        CommandLine cmdLine = CommandLine.parse(
                String.format("pmeter " + MEASURE + " %s --user %s --measure %s %s --file_name %s",
                        pmeterNic, odsUser,
                        measureCount, pmeterOptions, pmeterReportPath));
        this.cmdLine = cmdLine;
        logger.info(this.cmdLine.toString());
    }


    public void runPmeter() {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PumpStreamHandler streamHandler = new PumpStreamHandler(outputStream);

        DefaultExecutor executor = new DefaultExecutor();

        ExecuteWatchdog watchDog = new ExecuteWatchdog(ExecuteWatchdog.INFINITE_TIMEOUT);
        executor.setWatchdog(watchDog);
        executor.setStreamHandler(streamHandler);
        try {
            executor.execute(cmdLine);
        } catch (IOException e) {
            logger.error("Failed in executing pmeter script:\n " + cmdLine);
            e.printStackTrace();
        }
    }

    public List<DataInflux> parsePmeterOutput() throws IOException {
        Path path = Paths.get(pmeterReportPath);
        List<String> allLines = Files.readAllLines(path);
        List<DataInflux> ret = new ArrayList<>();

        for (String line : allLines) {
            System.out.println("COMING");
            System.out.println(line);

            DataInflux dataInflux = this.pmeterMapper.readValue(line, DataInflux.class);
            processLineAndUpdateInflux(dataInflux);
            ret.add(dataInflux);
        }
        path.toFile().delete();
        path.toFile().createNewFile();
        return ret;
    }

    private void processLineAndUpdateInflux(DataInflux dataInflux) {
        Iterable<Tag> tags = List.of(
                Tag.of(DataInfluxConstants.CPU_ARCHITECTURE, dataInflux.getCpuArchitecture()),
                Tag.of(DataInfluxConstants.ODS_USER, dataInflux.getOdsUser()),
                Tag.of(DataInfluxConstants.TRANSFER_NODE_NAME, dataInflux.getTransferNodeName()),
                Tag.of(DataInfluxConstants.SOURCE_TYPE, dataInflux.getSourceType()),
                Tag.of(DataInfluxConstants.DESTINATION_TYPE, dataInflux.getDestType())
        );

        if(Objects.nonNull(dataInflux.getCpu_frequency_current())) {
            Metrics.gauge(DataInfluxConstants.CPU_FREQUENCY_CURRENT, dataInflux.getCpu_frequency_current());
        }
        if(Objects.nonNull(dataInflux.getCpu_frequency_max())) {
            Metrics.gauge(DataInfluxConstants.CPU_FREQUENCY_MAX, dataInflux.getCpu_frequency_max());
        }
        if(Objects.nonNull(dataInflux.getCpu_frequency_min())) {
            Metrics.gauge(DataInfluxConstants.CPU_FREQUENCY_MIN, dataInflux.getCpu_frequency_min());
        }
        if(Objects.nonNull(dataInflux.getCoreCount())) {
            Metrics.gauge(DataInfluxConstants.ACTIVE_CORE_COUNT, dataInflux.getCoreCount());
        }
        if(Objects.nonNull(dataInflux.getBytesReceived())) {
            Metrics.gauge(DataInfluxConstants.BYTES_RECEIVED, dataInflux.getBytesReceived());
        }
        if(Objects.nonNull(dataInflux.getBytesSent())) {
            Metrics.gauge(DataInfluxConstants.BYTES_SENT, dataInflux.getBytesSent());
        }
        if(Objects.nonNull(dataInflux.getDropin())) {
            Metrics.gauge(DataInfluxConstants.DROP_IN, dataInflux.getDropin());
        }
        if(Objects.nonNull(dataInflux.getDropout())) {
            Metrics.gauge(DataInfluxConstants.DROP_OUT, dataInflux.getDropout());
        }
        if(Objects.nonNull(dataInflux.getEnergyConsumed())) {
            Metrics.gauge(DataInfluxConstants.ENERGY_CONSUMED, dataInflux.getEnergyConsumed());
        }
        if(Objects.nonNull(dataInflux.getErrin())) {
            Metrics.gauge(DataInfluxConstants.ERROR_IN, dataInflux.getErrin());
        }
        if(Objects.nonNull(dataInflux.getErrout())) {
            Metrics.gauge(DataInfluxConstants.ERROR_OUT, dataInflux.getErrout());
        }
        if(Objects.nonNull(dataInflux.getLatency())) {
            Metrics.timer(DataInfluxConstants.LATENCY).record(dataInflux.getLatency().longValue(), TimeUnit.MILLISECONDS);
        }
        if(Objects.nonNull(dataInflux.getLinkCapacity())) {
            Metrics.gauge(DataInfluxConstants.LINK_CAPACITY, dataInflux.getLinkCapacity());
        }
        if(Objects.nonNull(dataInflux.getNicMtu())) {
            Metrics.gauge(DataInfluxConstants.NIC_MTU, dataInflux.getNicMtu());
        }
        if(Objects.nonNull(dataInflux.getNicSpeed())) {
            Metrics.gauge(DataInfluxConstants.NIC_SPEED, dataInflux.getNicSpeed());
        }
        if(Objects.nonNull(dataInflux.getPacketLossRate())) {
            Metrics.gauge(DataInfluxConstants.PACKET_LOSS_RATE, dataInflux.getPacketLossRate());
        }
        if(Objects.nonNull(dataInflux.getPacketReceived())) {
            Metrics.gauge(DataInfluxConstants.PACKETS_RECEIVED, dataInflux.getPacketReceived());
        }
        if(Objects.nonNull(dataInflux.getPacketSent())) {
            Metrics.gauge(DataInfluxConstants.PACKETS_SENT, dataInflux.getPacketSent());
        }
        if(Objects.nonNull(dataInflux.getRtt())) {
            Metrics.timer(DataInfluxConstants.RTT).record(dataInflux.getRtt().longValue(), TimeUnit.MILLISECONDS);
        }
        if(Objects.nonNull(dataInflux.getConcurrency())) {
            Metrics.gauge(DataInfluxConstants.CONCURRENCY, dataInflux.getConcurrency());
        }
        if(Objects.nonNull(dataInflux.getParallelism())) {
            Metrics.gauge(DataInfluxConstants.PARALLELISM, dataInflux.getParallelism());
        }
        if(Objects.nonNull(dataInflux.getDataBytesSent())) {
            Metrics.gauge(DataInfluxConstants.TOTAL_BYTES_SENT, dataInflux.getDataBytesSent());
        }
        if(Objects.nonNull(dataInflux.getThroughput())) {
            Metrics.gauge(DataInfluxConstants.THROUGHPUT, dataInflux.getThroughput());
        }
        if(Objects.nonNull(dataInflux.getMemory())) {
            Metrics.gauge(DataInfluxConstants.MEMORY, dataInflux.getMemory());
        }
        if(Objects.nonNull(dataInflux.getMaxMemory())) {
            Metrics.gauge(DataInfluxConstants.MAX_MEMORY, dataInflux.getMaxMemory());
        }
        if(Objects.nonNull(dataInflux.getFreeMemory())) {
            Metrics.gauge(DataInfluxConstants.FREE_MEMORY, dataInflux.getFreeMemory());
        }
        if(Objects.nonNull(dataInflux.getAllocatedMemory())) {
            Metrics.gauge(DataInfluxConstants.ALLOCATED_MEMORY, dataInflux.getAllocatedMemory());
        }

    }
}
