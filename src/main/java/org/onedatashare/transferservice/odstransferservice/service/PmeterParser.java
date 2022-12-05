package org.onedatashare.transferservice.odstransferservice.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.AtomicDouble;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.PumpStreamHandler;
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
import java.util.concurrent.atomic.AtomicLong;

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

    private AtomicDouble memory = new AtomicDouble(0L);
    private AtomicDouble cpuFrequencyMax = new AtomicDouble(0L);
    private AtomicDouble cpuFrequencyMin = new AtomicDouble(0L);
    private AtomicDouble activeCoreCount = new AtomicDouble(0L);
    private AtomicDouble bytesReceived = new AtomicDouble(0L);
    private AtomicDouble bytesSent = new AtomicDouble(0L);
    private AtomicDouble dropIn = new AtomicDouble(0L);
    private AtomicDouble dropOut = new AtomicDouble(0L);
    private AtomicDouble energyConsumed = new AtomicDouble(0L);
    private AtomicDouble errorIn = new AtomicDouble(0L);
    private AtomicDouble errorOut = new AtomicDouble(0L);
    private AtomicDouble linkCapacity = new AtomicDouble(0L);
    private AtomicDouble nicMtu = new AtomicDouble(0L);
    private AtomicDouble nicSpeed = new AtomicDouble(0L);
    private AtomicDouble packetLossRate = new AtomicDouble(0L);
    private AtomicDouble packetReceived = new AtomicDouble(0L);
    private AtomicDouble packetSent = new AtomicDouble(0L);
    private AtomicDouble concurrency = new AtomicDouble(0L);
    private AtomicDouble parallelism = new AtomicDouble(0L);
    private AtomicDouble totalBytesSent = new AtomicDouble(0L);
    private AtomicDouble maxMemory = new AtomicDouble(0L);
    private AtomicDouble freeMemory = new AtomicDouble(0L);
    private AtomicDouble allocatedMemory = new AtomicDouble(0L);

    private AtomicLong rtt = new AtomicLong(0L);
    private AtomicLong latency = new AtomicLong(0L);

    @PostConstruct
    public void postConstruct(){
        CommandLine cmdLine = CommandLine.parse(
                String.format("pmeter " + MEASURE + " %s --user %s --measure %s %s --file_name %s",
                        pmeterNic, odsUser,
                        measureCount, pmeterOptions, pmeterReportPath));
        this.cmdLine = cmdLine;
        logger.info(this.cmdLine.toString());

        Metrics.gauge(DataInfluxConstants.MEMORY, memory);
        Metrics.gauge(DataInfluxConstants.CPU_FREQUENCY_MAX, cpuFrequencyMax);
        Metrics.gauge(DataInfluxConstants.CPU_FREQUENCY_MIN, cpuFrequencyMin);
        Metrics.gauge(DataInfluxConstants.ACTIVE_CORE_COUNT, activeCoreCount);
        Metrics.gauge(DataInfluxConstants.BYTES_RECEIVED, bytesReceived);
        Metrics.gauge(DataInfluxConstants.BYTES_SENT, bytesSent);
        Metrics.gauge(DataInfluxConstants.DROP_IN, dropIn);
        Metrics.gauge(DataInfluxConstants.DROP_OUT, dropOut);
        Metrics.gauge(DataInfluxConstants.ENERGY_CONSUMED, energyConsumed);
        Metrics.gauge(DataInfluxConstants.ERROR_IN, errorIn);
        Metrics.gauge(DataInfluxConstants.ERROR_OUT, errorOut);
        Metrics.gauge(DataInfluxConstants.LINK_CAPACITY, linkCapacity);
        Metrics.gauge(DataInfluxConstants.NIC_MTU, nicMtu);
        Metrics.gauge(DataInfluxConstants.NIC_SPEED, nicSpeed);
        Metrics.gauge(DataInfluxConstants.PACKET_LOSS_RATE, packetLossRate);
        Metrics.gauge(DataInfluxConstants.PACKETS_RECEIVED, packetReceived);
        Metrics.gauge(DataInfluxConstants.PACKETS_SENT, packetSent);
        Metrics.gauge(DataInfluxConstants.CONCURRENCY, concurrency);
        Metrics.gauge(DataInfluxConstants.PARALLELISM, parallelism);
        Metrics.gauge(DataInfluxConstants.TOTAL_BYTES_SENT, totalBytesSent);
        Metrics.gauge(DataInfluxConstants.MAX_MEMORY, maxMemory);
        Metrics.gauge(DataInfluxConstants.FREE_MEMORY, freeMemory);
        Metrics.gauge(DataInfluxConstants.ALLOCATED_MEMORY, allocatedMemory);

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
                Tag.of(DataInfluxConstants.CPU_ARCHITECTURE, dataInflux.getCpuArchitecture())
//                Tag.of(DataInfluxConstants.ODS_USER, dataInflux.getOdsUser()),
//                Tag.of(DataInfluxConstants.TRANSFER_NODE_NAME, dataInflux.getTransferNodeName()),
//                Tag.of(DataInfluxConstants.SOURCE_TYPE, dataInflux.getSourceType()),
//                Tag.of(DataInfluxConstants.DESTINATION_TYPE, dataInflux.getDestType())
        );

//        Metrics.summary(DataInfluxConstants.MEMORY, tags).record(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());

        memory.set(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());
        cpuFrequencyMax.set(dataInflux.getCpu_frequency_max());
        cpuFrequencyMin.set(dataInflux.getCpu_frequency_min());
        activeCoreCount.set(dataInflux.getCoreCount());
        bytesReceived.set(dataInflux.getBytesReceived());
        if (Objects.nonNull(dataInflux.getBytesSent())) {
            bytesSent.set(dataInflux.getBytesSent());
        }

        if (Objects.nonNull(dataInflux.getDropin())) {
            dropIn.set(dataInflux.getDropin());
        }

        dropOut.set(dataInflux.getDropout());
        energyConsumed.set(dataInflux.getEnergyConsumed());
        errorIn.set(dataInflux.getErrin());
        errorOut.set(dataInflux.getErrout());
        linkCapacity.set(dataInflux.getLinkCapacity());
        nicMtu.set(dataInflux.getNicMtu());
        nicSpeed.set(dataInflux.getNicSpeed());
        packetLossRate.set(dataInflux.getPacketLossRate());
        packetReceived.set(dataInflux.getPacketReceived());
        packetSent.set(dataInflux.getPacketSent());

        if (Objects.nonNull(dataInflux.getConcurrency())) {
            concurrency.set(dataInflux.getConcurrency());
        }

        if (Objects.nonNull(dataInflux.getParallelism())) {
            parallelism.set(dataInflux.getParallelism());
        }

        if (Objects.nonNull(dataInflux.getDataBytesSent())) {
            totalBytesSent.set(dataInflux.getDataBytesSent());
        }

        if (Objects.nonNull(dataInflux.getMaxMemory())) {
            maxMemory.set(dataInflux.getMaxMemory());
        }

        if (Objects.nonNull(dataInflux.getFreeMemory())) {
            freeMemory.set(dataInflux.getFreeMemory());
        }

        if (Objects.nonNull(dataInflux.getAllocatedMemory())) {
            allocatedMemory.set(dataInflux.getAllocatedMemory());
        }

        if(Objects.nonNull(dataInflux.getRtt())) {
            Metrics.timer(DataInfluxConstants.RTT).record(dataInflux.getRtt().longValue(), TimeUnit.MILLISECONDS);
        }

        if(Objects.nonNull(dataInflux.getLatency())) {
            Metrics.timer(DataInfluxConstants.LATENCY).record(dataInflux.getLatency().longValue(), TimeUnit.MILLISECONDS);
        }
    }
}
