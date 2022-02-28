package org.onedatashare.transferservice.odstransferservice.controller;

import com.google.gson.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.exec.*;
import org.onedatashare.transferservice.odstransferservice.cron.metric.NetworkMetric;
import org.onedatashare.transferservice.odstransferservice.model.MetaDataDTO;
import org.onedatashare.transferservice.odstransferservice.service.DatabaseService.CrudService;
import org.onedatashare.transferservice.odstransferservice.service.DatabaseService.metric.NetworkMetricServiceImpl;
import org.onedatashare.transferservice.odstransferservice.utility.DataUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
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

    //todo - env variable
    private static final String SCRIPT_PATH = "/Users/DG/Documents/Courses/Spring22/PDP/pmeter/src/pmeter/pmeter_cli.py";
    private static final String REPORT_PATH = "pmeter_measure.txt";

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
//            executeScript();
            log.info("Read file");
            NetworkMetric networkMetric = readFile();
            log.info("Save to db");
            saveData(networkMetric);
        }catch (Exception e){
            e.printStackTrace();
            log.error("Exception encountered while running cron");
        }

    }

    public static void main(String[] args) throws ParseException {
        MetricsCollector metricsCollector = new MetricsCollector();
        String s = "2022-02-19 19:07:02.012633";
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);
        Date date = format.parse(s);
        System.out.println(format.format(date));
        System.out.println(date.getTime());
        //metricsCollector.saveData(null);
    }

    private void saveData(NetworkMetric networkMetric){
        networkMetricService.saveOrUpdate(networkMetric);
    }

    //python3 src/pmeter/pmeter_cli.py measure eth0 -K
    private void executeScript() throws Exception {
        String line = "python3 " + SCRIPT_PATH;
        CommandLine cmdLine = CommandLine.parse(line);
        cmdLine.addArgument("measure");
        cmdLine.addArgument("eth0");
        cmdLine.addArgument("-K");

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PumpStreamHandler streamHandler = new PumpStreamHandler(outputStream);

        DefaultExecutor executor = new DefaultExecutor();

        ExecuteWatchdog watchDog = new ExecuteWatchdog(ExecuteWatchdog.INFINITE_TIMEOUT);
        executor.setWatchdog(watchDog);
        executor.setStreamHandler(streamHandler);

        try {
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

        /** Reading a json array

        try (JsonReader reader = new JsonReader(new FileReader("pmeter_measure.txt"))){

            // convert JSON file to map
            JsonArray array = JsonParser.parseReader(reader).getAsJsonArray();

            List<Map<?, ?>> metricList = new ArrayList<>();
            for (JsonElement metric : array) { //cropDetails is the JSONArray
                Map<?, ?> map = gson.fromJson(metric, Map.class);
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    System.out.println(entry.getKey() + "=" + entry.getValue());
                }
                metricList.add(map);
            }



            //Reading a single json object
            Map<?, ?> map = gson.fromJson(reader, Map.class);
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                System.out.println(entry.getKey() + "=" + entry.getValue());
            }


        } catch (IOException e) {
            log.error("Exception occurred while reading file");
            e.printStackTrace();
        }
        **/

        try(Reader r = new InputStreamReader( new FileInputStream(REPORT_PATH))) {
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

        log.info("Read contents of pmeter_metric.txt");
        return networkMetric;
    }


}