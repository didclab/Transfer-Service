package org.onedatashare.transferservice.odstransferservice.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.PumpStreamHandler;
import org.onedatashare.transferservice.odstransferservice.model.metrics.DataInflux;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

@Service
public class PmeterParser {

    private final String MEASURE = "measure";
    private final ByteArrayOutputStream outputStream;
    private final PumpStreamHandler streamHandler;
    private final DefaultExecutor executor;
    private final ExecuteWatchdog watchDog;

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


    private CommandLine cmdLine;

    public PmeterParser(ObjectMapper pmeterMapper) {
        this.outputStream = new ByteArrayOutputStream();
        this.streamHandler = new PumpStreamHandler(outputStream);

        this.executor = new DefaultExecutor();

        this.watchDog = new ExecuteWatchdog(ExecuteWatchdog.INFINITE_TIMEOUT);
        executor.setWatchdog(watchDog);
        executor.setStreamHandler(streamHandler);

        this.pmeterMapper = pmeterMapper;
    }

    @PostConstruct
    public void postConstruct() {
        this.cmdLine = CommandLine.parse(
                String.format("pmeter " + MEASURE + " %s --user %s --measure %s %s --file_name %s",
                        pmeterNic, odsUser,
                        measureCount, pmeterOptions, pmeterReportPath));
    }


    public void runPmeter() {
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
            ret.add(dataInflux);
        }
        path.toFile().delete();
        path.toFile().createNewFile();
        return ret;
    }
}
