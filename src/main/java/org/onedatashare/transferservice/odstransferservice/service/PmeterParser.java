package org.onedatashare.transferservice.odstransferservice.service;

import com.amazonaws.util.EC2MetadataUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.AtomicDouble;
import com.netflix.discovery.converters.Auto;
import com.netflix.servo.util.ThreadCpuStats;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.influx.InfluxMeterRegistry;
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
import javax.management.MXBean;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.net.NetworkInterface;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class PmeterParser {

    private final String MEASURE = "measure";

    Logger logger = LoggerFactory.getLogger(PmeterParser.class);


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
    ObjectMapper pmeterMapper;

    InfluxMeterRegistry influxMeterRegistry;

    private CommandLine cmdLine;

    private AtomicDouble cpuFrequencyMax;
    private AtomicDouble cpuFrequencyMin;
    private AtomicLong bytesReceived;
    private AtomicLong bytesSent;
    private AtomicLong dropIn;
    private AtomicLong dropOut;
    private AtomicLong errorIn;
    private AtomicLong errorOut;
    private AtomicDouble linkCapacity;
    private AtomicInteger nicMtu;
    private AtomicLong nicSpeed;
    private AtomicLong packetReceived;
    private AtomicLong packetSent;
    private AtomicDouble rtt;
    private AtomicDouble latency;

    public PmeterParser(ObjectMapper pmeterMapper, InfluxMeterRegistry influxMeterRegistry) {
        cpuFrequencyMax = new AtomicDouble(0L);
        cpuFrequencyMin = new AtomicDouble(0L);
        bytesReceived = new AtomicLong(0L);
        bytesSent = new AtomicLong(0L);
        dropIn = new AtomicLong(0L);
        dropOut = new AtomicLong(0L);
        errorIn = new AtomicLong(0L);
        errorOut = new AtomicLong(0L);
        linkCapacity = new AtomicDouble(0L);
        nicMtu = new AtomicInteger(0);
        nicSpeed = new AtomicLong(0L);
        packetReceived = new AtomicLong(0L);
        packetSent = new AtomicLong(0L);
        rtt = new AtomicDouble(0L);
        latency = new AtomicDouble(0L);
        this.pmeterMapper = pmeterMapper;
        this.influxMeterRegistry = influxMeterRegistry;
    }

    @PostConstruct
    public void postConstruct() {
        this.cmdLine = CommandLine.parse(
                String.format("pmeter " + MEASURE + " %s --user %s --measure %s %s --file_name %s",
                        pmeterNic, odsUser,
                        measureCount, pmeterOptions, pmeterReportPath));
        Gauge.builder(DataInfluxConstants.CPU_FREQUENCY_MAX, cpuFrequencyMax, AtomicDouble::doubleValue)
                .baseUnit("Mhz")
                .register(this.influxMeterRegistry);
        Gauge.builder(DataInfluxConstants.CPU_FREQUENCY_MIN, cpuFrequencyMin, AtomicDouble::doubleValue)
                .baseUnit("Mhz")
                .register(this.influxMeterRegistry);
        Gauge.builder(DataInfluxConstants.BYTES_RECEIVED, bytesReceived, AtomicLong::longValue)
                .baseUnit("bytes")
                .register(this.influxMeterRegistry);
        Gauge.builder(DataInfluxConstants.BYTES_SENT, bytesSent, AtomicLong::longValue)
                .baseUnit("bytes")
                .register(this.influxMeterRegistry);
        Gauge.builder(DataInfluxConstants.BYTES_SENT, bytesSent, AtomicLong::longValue)
                .baseUnit("bytes")
                .register(this.influxMeterRegistry);
        Gauge.builder(DataInfluxConstants.DROP_IN, dropIn, AtomicLong::longValue)
                .baseUnit("packets")
                .register(this.influxMeterRegistry);
        Gauge.builder(DataInfluxConstants.DROP_OUT, dropOut, AtomicLong::longValue)
                .baseUnit("packets")
                .register(this.influxMeterRegistry);
        Gauge.builder(DataInfluxConstants.ERROR_IN, errorIn, AtomicLong::longValue)
                .baseUnit("errors")
                .register(this.influxMeterRegistry);
        Gauge.builder(DataInfluxConstants.ERROR_OUT, errorOut, AtomicLong::longValue)
                .baseUnit("errors")
                .register(this.influxMeterRegistry);
        Gauge.builder(DataInfluxConstants.NIC_MTU, nicMtu, AtomicInteger::intValue)
                .baseUnit("bytes")
                .register(this.influxMeterRegistry);
        Gauge.builder(DataInfluxConstants.NIC_SPEED, nicSpeed, AtomicLong::longValue)
                .baseUnit("megabits")
                .register(this.influxMeterRegistry);
        Gauge.builder(DataInfluxConstants.PACKETS_RECEIVED, packetReceived, AtomicLong::longValue)
                .baseUnit("packets")
                .register(this.influxMeterRegistry);
        Gauge.builder(DataInfluxConstants.PACKETS_SENT, packetSent, AtomicLong::longValue)
                .baseUnit("packets")
                .register(this.influxMeterRegistry);
        Gauge.builder(DataInfluxConstants.RTT, rtt, AtomicDouble::doubleValue)
                .baseUnit("seconds")
                .register(this.influxMeterRegistry);
        Gauge.builder(DataInfluxConstants.LATENCY, latency, AtomicDouble::doubleValue)
                .baseUnit("seconds")
                .register(this.influxMeterRegistry);
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
            DataInflux dataInflux = this.pmeterMapper.readValue(line, DataInflux.class);
            processLineAndUpdateInflux(dataInflux);
            ret.add(dataInflux);
        }
        path.toFile().delete();
        path.toFile().createNewFile();
        return ret;
    }

    private void processLineAndUpdateInflux(DataInflux dataInflux) {
        cpuFrequencyMax.set(dataInflux.getCpu_frequency_max());
        cpuFrequencyMin.set(dataInflux.getCpu_frequency_min());
        bytesReceived.set(dataInflux.getBytesReceived());
        bytesSent.set(dataInflux.getBytesSent());
        dropIn.set(dataInflux.getDropin());
        dropOut.set(dataInflux.getDropout());
        errorIn.set(dataInflux.getErrin());
        errorOut.set(dataInflux.getErrout());
        linkCapacity.set(dataInflux.getLinkCapacity());
        nicMtu.set(dataInflux.getNicMtu());
        nicSpeed.set(dataInflux.getNicSpeed());
        packetReceived.set(dataInflux.getPacketReceived());
        packetSent.set(dataInflux.getPacketSent());
        rtt.set(dataInflux.getRtt());
        latency.set(dataInflux.getLatency());
    }
}
