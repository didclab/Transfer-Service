package org.onedatashare.transferservice.odstransferservice.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.PumpStreamHandler;
import org.onedatashare.transferservice.odstransferservice.model.metrics.CarbonScore;
import org.onedatashare.transferservice.odstransferservice.model.metrics.DataInflux;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

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
    private final DefaultExecutor pmeterExecutor;
    private final ExecuteWatchdog watchDog;

    Logger logger = LoggerFactory.getLogger(PmeterParser.class);

    @Value("${pmeter.carbon.path}")
    String pmeterCarbonPath;

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

    @PostConstruct
    public void init() {
        this.cmdLine = CommandLine.parse(
                String.format("pmeter " + MEASURE + " %s --user %s --measure %s %s --file_name %s",
                        pmeterNic, odsUser,
                        measureCount, pmeterOptions, pmeterReportPath));
    }

    public PmeterParser(ObjectMapper pmeterMapper) {
        this.outputStream = new ByteArrayOutputStream();
        this.streamHandler = new PumpStreamHandler(outputStream);

        this.pmeterExecutor = new DefaultExecutor();
        this.watchDog = new ExecuteWatchdog(ExecuteWatchdog.INFINITE_TIMEOUT);
        pmeterExecutor.setWatchdog(watchDog);
        pmeterExecutor.setStreamHandler(streamHandler);

        this.pmeterMapper = pmeterMapper;
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

    public CarbonScore runCarbonPmeter(String ip){
        //pmeter carbon 129.114.108.45
         CommandLine carbonCmd= CommandLine.parse(String.format("pmeter carbon %s", ip));
        try {
            DefaultExecutor carbonExecutor = new DefaultExecutor();
            carbonExecutor.execute(carbonCmd);
        } catch (IOException e) {
            return new CarbonScore();
        }
        try {
            Path filePath = Paths.get(this.pmeterCarbonPath);
            List<String> lines = Files.readAllLines(filePath);
            CarbonScore score = new CarbonScore();
            for(String line: lines){
                score = this.pmeterMapper.readValue(line, CarbonScore.class);
                break;
            }
            filePath.toFile().delete();
            filePath.toFile().createNewFile();
            return score;
        } catch (IOException e) {
            return new CarbonScore();
        }
    }
}
