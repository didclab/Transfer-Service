package org.onedatashare.transferservice.odstransferservice.service.cron;

import com.google.gson.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.SneakyThrows;
import org.apache.commons.exec.*;
import org.json.JSONArray;
import org.json.JSONObject;
import org.onedatashare.transferservice.odstransferservice.DataRepository.NetworkMetricsInfluxRepository;
import org.onedatashare.transferservice.odstransferservice.config.CommandLineOptions;
import org.onedatashare.transferservice.odstransferservice.constant.ODSConstants;
import org.onedatashare.transferservice.odstransferservice.model.NetworkMetric;
import org.onedatashare.transferservice.odstransferservice.model.metrics.DataInflux;
import org.onedatashare.transferservice.odstransferservice.model.metrics.NetworkMetricInflux;
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
    private static final String PYTHON3="python3";
    private static final String MEASURE = "measure";

    static long bytesSentOld = 0;
    static long bytesReceivedOld  = 0;

    static long packetsSentOld = 0;
    static long packetsReceivedOld  = 0;

    @Autowired
    private NetworkMetricServiceImpl networkMetricService;

    @Autowired
    private NetworkMetricInflux networkMetricInflux;

    @Autowired
    private NetworkMetricsInfluxRepository repo;

    @Autowired
    private CommandLineOptions cmdLineOptions;

    @Autowired
    private NetworkMetric networkMetric;

    @Autowired
    private DataInflux dataInflux;

    /**
     * Running every 10 minutes
     * 1. Execute pmeter script
     * 2. Read file
     * 3. Push to db
     */
    @Scheduled(cron = "0 0/1 * * * *")
    public void collectAndSave() {
        try {
            executeScript();
            NetworkMetric networkMetric = readFile();
            saveData(networkMetric);
            mapper(networkMetric);
            repo.insertDataPoints(dataInflux);
            if(log.isDebugEnabled())
                log.debug("Pushed data: "+ networkMetricInflux.toString());
        }catch (Exception e){
            e.printStackTrace();
            log.error("Exception encountered while running cron");
        }

    }

    private void saveData(NetworkMetric networkMetric){
        networkMetricService.saveOrUpdate(networkMetric);
    }

    //python3 src/pmeter/pmeter_cli.py measure en0 --user jgoldverg@gmail.com --length -1s --measure 10 -KNS
    @SneakyThrows
    private void executeScript() throws Exception {

        CommandLine cmdLine = CommandLine.parse(
                String.format("sudo python3 %s " + MEASURE +" %s --user %s --length %s %s",
                        SCRIPT_PATH, cmdLineOptions.getNetworkInterface(), cmdLineOptions.getUser(),
                        cmdLineOptions.getLength(), cmdLineOptions.getOptions()));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PumpStreamHandler streamHandler = new PumpStreamHandler(outputStream);

        DefaultExecutor executor = new DefaultExecutor();

        ExecuteWatchdog watchDog = new ExecuteWatchdog(ExecuteWatchdog.INFINITE_TIMEOUT);
        executor.setWatchdog(watchDog);
        executor.setStreamHandler(streamHandler);
        executor.execute(cmdLine);
        //log.info(outputStream.toString());
    }

    /**
     * todo - parameterize
     * @return
     */
    private NetworkMetric readFile() {
        Date startTime = null;
        Date endTime = null;

        File inputFile = new File(ODSConstants.PMETER_REPORT_PATH);
        File tempFile = new File(ODSConstants.PMETER_TEMP_REPORT);

        try(Reader r = new InputStreamReader(new FileInputStream(inputFile))
        ) {
            tempFile.createNewFile();
            JsonStreamParser p = new JsonStreamParser(r);
            List<Map<?, ?>> metricList = new ArrayList<>();
            while (p.hasNext()) {
                JsonElement metric = p.next();
                if (metric.isJsonObject()) {
                    Map<?, ?> map = new Gson().fromJson(metric, Map.class);
                    for (Map.Entry<?, ?> entry : map.entrySet()) {
                       log.info(entry.getKey() + "=" + entry.getValue());
                    }
                    startTime = DataUtil.getDate((String)map.get("start_time"));
                    endTime = DataUtil.getDate((String)map.get("end_time"));
                    metricList.add(map);
                }
            }
            networkMetric.setData(new Gson().toJson(metricList));
            networkMetric.setStartTime(startTime);
            networkMetric.setEndTime(endTime);

        }catch (IOException | ParseException e){
            log.error("Exception occurred while reading file",e);
        }
        inputFile.delete();
        tempFile.renameTo(inputFile);

        return networkMetric;
    }

    public DataInflux mapper(NetworkMetric nw){

        networkMetricInflux.setTime(Instant.now());
        if(nw.getData()!=null) {
            String json = nw.getData();
            GsonBuilder gsonBuilder = new GsonBuilder();
            gsonBuilder.setDateFormat("yyyy-MM-dd HH:mm:ss");
            gsonBuilder.setLongSerializationPolicy(LongSerializationPolicy.STRING);
            DataInflux[] dataArr = gsonBuilder.create().fromJson(json, DataInflux[].class);
            //networkMetricInflux.setData(dataArr);
            dataInflux = dataArr[dataArr.length-1];
            float[] l = dataInflux.getLatencyArr();
            dataInflux.setLatencyVal(l[0]);
            if(bytesSentOld!=0) {
                long bytesSentDelta = dataInflux.getBytesSent()-bytesSentOld;
                bytesSentOld=dataInflux.getBytesSent();
                dataInflux.setBytesSent(bytesSentDelta);
            }else {
                long temp =dataInflux.getBytesSent();
                dataInflux.setBytesSent(bytesSentOld);
                bytesSentOld=temp;

            }
            if(bytesReceivedOld!=0) {
                long bytesReceivedDelta = dataInflux.getBytesReceived()-bytesReceivedOld;
                bytesReceivedOld=dataInflux.getBytesReceived();
                dataInflux.setBytesReceived(bytesReceivedDelta);
            }else {
                long temp =dataInflux.getBytesReceived();
                dataInflux.setBytesReceived(bytesReceivedOld);
                bytesReceivedOld=temp;
            }

            if(packetsSentOld!=0) {
                long packetsSentDelta = dataInflux.getPacketSent()-packetsSentOld;
                packetsSentOld=dataInflux.getPacketSent();
                dataInflux.setPacketSent(packetsSentDelta);
            }else {
                long temp =dataInflux.getPacketSent();
                dataInflux.setPacketSent(packetsSentOld);
                packetsSentOld=temp;

            }
            if(packetsReceivedOld!=0) {
                long packetsReceivedDelta = dataInflux.getPacketReceived()-packetsReceivedOld;
                packetsReceivedOld=dataInflux.getPacketReceived();
                dataInflux.setPacketReceived(packetsReceivedDelta);
            }else {
                long temp =dataInflux.getPacketReceived();
                dataInflux.setPacketReceived(packetsReceivedOld);
                packetsReceivedOld=temp;
            }

            System.out.println("Bytes sent: " + dataInflux.getBytesSent());
            System.out.println("Bytes Received: " + dataInflux.getBytesReceived());
            System.out.println("Packets sent: " + dataInflux.getPacketSent());
            System.out.println("Packets Received: " + dataInflux.getPacketReceived());
        }

        if(nw.getStartTime()!= null)
            networkMetricInflux.setStart_time(nw.getStartTime());
        if(nw.getEndTime()!= null)
            networkMetricInflux.setEnd_time(nw.getEndTime());
        return dataInflux;
    }

}
