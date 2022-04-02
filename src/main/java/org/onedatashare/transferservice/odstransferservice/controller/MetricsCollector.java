package org.onedatashare.transferservice.odstransferservice.controller;

import com.google.gson.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.exec.*;
import org.onedatashare.transferservice.odstransferservice.DataRepository.NetworkMetricsInfluxRepository;
import org.onedatashare.transferservice.odstransferservice.cron.metric.NetworkMetric;
import org.onedatashare.transferservice.odstransferservice.model.NetworkMetricInflux;
import org.onedatashare.transferservice.odstransferservice.service.DatabaseService.metric.NetworkMetricServiceImpl;
import org.onedatashare.transferservice.odstransferservice.utility.DataUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.*;
import java.text.ParseException;
import java.time.Instant;
import java.util.*;

/**
 * @author deepika
 */
@Service
@NoArgsConstructor
@Getter
@Setter
public class MetricsCollector {

    private static final Logger log = LoggerFactory.getLogger(MetricsCollector.class);

    private static final String SCRIPT_PATH = System.getenv("PMETER_HOME") + "src/pmeter/pmeter_cli.py";
    private static final String REPORT_PATH = System.getenv("HOME") + "/.pmeter/pmeter_measure.txt";
    private static final String TEMP = "pmeter_measure_temp.txt";

    @Autowired
    NetworkMetricServiceImpl networkMetricService;

    /**
     * Running every 10 minutes
     * 1. Execute pmeter script
     * 2. Read file
     * 3. Push to db
     */
    @Scheduled(cron = "0 0/1 * * * *")
    public void collectAndSave() {
        try {
            log.info("Starting cron");
            log.info("Collecting network metrics");
            executeScript();
            log.info("Read file");
            NetworkMetric networkMetric = readFile();
            log.info("Save to db");
            saveData(networkMetric);
            NetworkMetricInflux networkMetricInflux= mapper(networkMetric);
            NetworkMetricsInfluxRepository repo= new NetworkMetricsInfluxRepository();
            repo.insertDataPoints(networkMetricInflux);
            log.info("Pushed data: "+ networkMetricInflux.toString());
        }catch (Exception e){
            e.printStackTrace();
            log.error("Exception encountered while running cron");
        }

    }

/*
    public static void main(String[] args) throws Exception {
        MetricsCollector metricsCollector = new MetricsCollector();
        metricsCollector.executeScript();
        metricsCollector.readFile();
    }
*/


    private void saveData(NetworkMetric networkMetric){
        networkMetricService.saveOrUpdate(networkMetric);
    }

    //python3 src/pmeter/pmeter_cli.py measure eth0 -K
    private void executeScript() throws Exception {
        String line = "python3 " + SCRIPT_PATH;
        CommandLine cmdLine = CommandLine.parse(line);
        cmdLine.addArgument("measure");
        cmdLine.addArgument("awdl0");
        cmdLine.addArgument("-K");
        cmdLine.addArgument("-N");
        //cmdLine.addArgument("-t");

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PumpStreamHandler streamHandler = new PumpStreamHandler(outputStream);

        DefaultExecutor executor = new DefaultExecutor();

        ExecuteWatchdog watchDog = new ExecuteWatchdog(ExecuteWatchdog.INFINITE_TIMEOUT);
        executor.setWatchdog(watchDog);
        executor.setStreamHandler(streamHandler);

        try{
            executor.execute(new CommandLine(cmdLine));
            log.info(outputStream.toString());
        } catch (IOException e) {
            log.info("Error occurred while executing network script");
            throw new Exception(e);
        }
    }

    private String resolvePythonScriptPath(String filename) {
        File file = new File(filename);
        return file.getAbsolutePath();
    }


    /**
     * todo - parameterize
     * @return
     */
    private NetworkMetric readFile(){
        NetworkMetric networkMetric = new NetworkMetric();
        Gson gson = new Gson();
        Date startTime = null;
        Date endTime = null;

        File inputFile = new File(REPORT_PATH);
        File tempFile = new File(TEMP);

        try(Reader r = new InputStreamReader(new FileInputStream(inputFile))
        ) {
            tempFile.createNewFile();
            JsonStreamParser p = new JsonStreamParser(r);
            List<Map<?, ?>> metricList = new ArrayList<>();
            while (p.hasNext()) {
                JsonElement metric = p.next();
                if (metric.isJsonObject()) {
                    Map<?, ?> map = gson.fromJson(metric, Map.class);
                    for (Map.Entry<?, ?> entry : map.entrySet()) {
                       log.info(entry.getKey() + "=" + entry.getValue());
                    }
                    startTime = DataUtil.getDate((String)map.get("start_time"));
                    endTime = DataUtil.getDate((String)map.get("end_time"));
                    metricList.add(map);
                }
            }
            networkMetric.setData(gson.toJson(metricList));
            networkMetric.setStartTime(startTime);
            networkMetric.setEndTime(endTime);

        }catch (IOException | ParseException e){
            log.error("Exception occurred while reading file",e);
        }
        inputFile.delete();
        tempFile.renameTo(inputFile);
        log.info("Read contents of pmeter_metric.txt");
        return networkMetric;
    }

    public NetworkMetricInflux mapper(NetworkMetric nw){
        NetworkMetricInflux nwf= new NetworkMetricInflux();
        nwf.setTime(Instant.now());
        if(nw.getData()!=null)
            nwf.setData(nw.getData());
        if(nw.getStartTime()!= null)
            nwf.setStart_time(nw.getStartTime());
        if(nw.getEndTime()!= null)
            nwf.setEnd_time(nw.getEndTime());
        return nwf;
    }

}
