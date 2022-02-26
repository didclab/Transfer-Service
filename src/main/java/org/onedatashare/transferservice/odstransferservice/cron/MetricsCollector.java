package org.onedatashare.transferservice.odstransferservice.cron;

import com.google.gson.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.exec.*;
import org.onedatashare.transferservice.odstransferservice.DataRepository.NetworkMetricRepository;
import org.onedatashare.transferservice.odstransferservice.config.ApplicationThreadPoolConfig;
import org.onedatashare.transferservice.odstransferservice.config.DataSourceConfig;
import org.onedatashare.transferservice.odstransferservice.cron.metric.NetworkMetric;
import org.onedatashare.transferservice.odstransferservice.model.MetaDataDTO;
import org.onedatashare.transferservice.odstransferservice.service.DatabaseService.MetaDataInterfaceImplementation;
import org.onedatashare.transferservice.odstransferservice.service.DatabaseService.metric.NetworkMetricService;
import org.onedatashare.transferservice.odstransferservice.service.DatabaseService.metric.NetworkMetricServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
    NetworkMetricRepository repository;

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
//            readFile();
            log.info("Save to db");
            saveData();
        }catch (Exception e){
            e.printStackTrace();
            log.error("Exception encountered while running cron");
        }

    }

    public static void main(String[] args) {
        MetricsCollector metricsCollector = new MetricsCollector();
        metricsCollector.saveData();
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
                        System.out.println(entry.getKey() + "=" + entry.getValue());
                    }
                    metricList.add(map);
                }
            }
        }catch (IOException e){
            log.error("Exception occurred while reading file");
        }

        log.info("Read contents of pmeter_metric.txt");
        return networkMetric;
    }

    private void saveData(){
        NetworkMetric networkMetric = new NetworkMetric();

        networkMetric.setId("1");
        MetaDataDTO dataDTO = new MetaDataDTO();
        dataDTO.setId("1");
//        metaDataServiceImplementation.saveOrUpdate(dataDTO);
        repository.save(networkMetric);
//        networkMetricService.saveOrUpdate(networkMetric);
        log.info("Saved");
    }
}