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
        if(Objects.nonNull(dataInflux.getCpu_frequency_current())) {
            Metrics.gauge(DataInfluxConstants.CPU_FREQUENCY_CURRENT, dataInflux.getCpu_frequency_current());
        }
        if(Objects.nonNull(dataInflux.getCpu_frequency_max())) {
            Metrics.gauge(DataInfluxConstants.CPU_FREQUENCY_MAX, dataInflux.getCpu_frequency_max());
        }
        if(Objects.nonNull(dataInflux.getCpu_frequency_min())) {
            Metrics.gauge(DataInfluxConstants.CPU_FREQUENCY_MIN, dataInflux.getCpu_frequency_min());
        }
    }
}
