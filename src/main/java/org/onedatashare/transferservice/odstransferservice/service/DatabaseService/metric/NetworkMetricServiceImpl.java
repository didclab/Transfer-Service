package org.onedatashare.transferservice.odstransferservice.service.DatabaseService.metric;

import com.amazonaws.util.StringUtils;
import com.google.gson.*;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.PumpStreamHandler;
import org.onedatashare.transferservice.odstransferservice.DataRepository.NetworkMetricRepository;
import org.onedatashare.transferservice.odstransferservice.config.CommandLineOptions;
import org.onedatashare.transferservice.odstransferservice.constant.ODSConstants;
import org.onedatashare.transferservice.odstransferservice.model.JobMetric;
import org.onedatashare.transferservice.odstransferservice.model.NetworkMetric;
import org.onedatashare.transferservice.odstransferservice.model.metrics.DataInflux;
import org.onedatashare.transferservice.odstransferservice.utility.DataUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.io.*;
import java.text.ParseException;
import java.util.*;

/**
 * @author deepika
 */
@Service
public class NetworkMetricServiceImpl implements NetworkMetricService {

    public static final String SINGLE_QUOTE = "'";
    private static final Logger LOG = LoggerFactory.getLogger(NetworkMetricService.class);
    private static final String TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss";

    static long bytesSentOld = 0;
    static long bytesReceivedOld  = 0;

    static long packetsSentOld = 0;
    static long packetsReceivedOld  = 0;


    @Autowired
    private NetworkMetricRepository repository;
    @Autowired
    private JdbcTemplate jdbcTemplate;
    @Autowired
    private CommandLineOptions cmdLineOptions;
    @Autowired
    private DataInflux dataInflux;

    private static final String SCRIPT_PATH = System.getenv("PMETER_HOME") + "src/pmeter/pmeter_cli.py";
    private static final String REPORT_PATH = System.getenv("HOME") + "/.pmeter/pmeter_measure.txt";
    private static final String TEMP = "pmeter_measure_temp.txt";
    private static final String PYTHON3="python3";
    private static final String MEASURE = "measure";

    @Override
    public NetworkMetric saveOrUpdate(NetworkMetric networkMetric) {
        try {
            LOG.info("Saving");
            StringBuilder stringBuilder = new StringBuilder("insert into network_metric (data, start_time, end_time) values(");
            stringBuilder.append("'");
            stringBuilder.append(networkMetric.getData());
            stringBuilder.append("',");
            stringBuilder.append(SINGLE_QUOTE);
            stringBuilder.append(DataUtil.getStringDate(networkMetric.getStartTime()));
            stringBuilder.append(SINGLE_QUOTE);
            stringBuilder.append(",");
            stringBuilder.append(SINGLE_QUOTE);
            stringBuilder.append(DataUtil.getStringDate(networkMetric.getEndTime()));
            stringBuilder.append(SINGLE_QUOTE);
            stringBuilder.append(")");
            LOG.info("sql insert: " + stringBuilder);
            jdbcTemplate.execute(stringBuilder.toString());

        }
        catch (Exception ex) {
            ex.getMessage();
        }
        return null;
    }

    @Override
    public List<NetworkMetric> readFile(String pmeterOutputFile) {

        boolean replaceWithEmptyFile = false;
        Date startTime = null;
        Date endTime = null;

        File inputFile = null;
        if(pmeterOutputFile == null){
            inputFile = new File(ODSConstants.PMeterConstants.PMETER_REPORT_PATH);
            replaceWithEmptyFile = true;
        }else{
            inputFile = new File(pmeterOutputFile);
        }

        File tempFile = new File(ODSConstants.PMeterConstants.PMETER_TEMP_REPORT);

        List<NetworkMetric> networkMetricList = new ArrayList<>();
        LOG.info("Reading file: " + inputFile);
        try(Reader r = new InputStreamReader(new FileInputStream(inputFile))
        ) {
            if(replaceWithEmptyFile) tempFile.createNewFile();
            JsonStreamParser p = new JsonStreamParser(r);
            List<Map<?, ?>> metricList = new ArrayList<>();
            while (p.hasNext()) {
                JsonElement metric = p.next();
                NetworkMetric networkMetric = new NetworkMetric();
                if (metric.isJsonObject()) {
                    Map<?, ?> map = new Gson().fromJson(metric, Map.class);
                    try {
                        String start = (String) map.get("start_time");
                        String end = (String) map.get("end_time");
                        if(!StringUtils.isNullOrEmpty(start))
                            startTime = DataUtil.getDate(start);
                        if(!StringUtils.isNullOrEmpty(end))
                            endTime = DataUtil.getDate(end);
                    }catch (Exception e){
                        LOG.info("Exception while reading file: " + inputFile);
                        LOG.info(metric.toString());
                        LOG.info((String) map.get("start_time"));
                        LOG.info((String) map.get("end_time"));
                        LOG.info(map.toString());
                        e.printStackTrace();
                    }
                    metricList.add(map);
                }
                networkMetric.setData(new Gson().toJson(metricList));
                networkMetric.setStartTime(startTime);
                networkMetric.setEndTime(endTime);
                networkMetricList.add(networkMetric);
            }

        }catch (EOFException e){
            LOG.error("Reached end of file",e);
        } catch (IOException e){
            LOG.error("Exception occurred while reading file",e);
        }
        inputFile.delete();
        if(replaceWithEmptyFile) tempFile.renameTo(inputFile);

        return networkMetricList;
    }

    @Override
    public void executeScript(String outputFile) throws Exception {
        if(outputFile == null){
            outputFile = ODSConstants.PMeterConstants.PMETER_REPORT_PATH;
        }
        //python3 src/pmeter/pmeter_cli.py measure en0 --user jgoldverg@gmail.com --length 1s --measure 10 -KNS
        CommandLine cmdLine = CommandLine.parse(
                String.format("python3 %s " + MEASURE +" %s --user %s --measure %s %s --file_name %s",
                        SCRIPT_PATH, cmdLineOptions.getNetworkInterface(), cmdLineOptions.getUser(),
                        cmdLineOptions.getMeasure(), cmdLineOptions.getOptions(), outputFile));

        LOG.info(cmdLine.toString());
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PumpStreamHandler streamHandler = new PumpStreamHandler(outputStream);

        DefaultExecutor executor = new DefaultExecutor();

        ExecuteWatchdog watchDog = new ExecuteWatchdog(ExecuteWatchdog.INFINITE_TIMEOUT);
        executor.setWatchdog(watchDog);
        executor.setStreamHandler(streamHandler);
        executor.execute(cmdLine);
        LOG.info("Script executed");
    }

    @Override
    public List<DataInflux> mapData(List<NetworkMetric> networkMetrics) {
        List<DataInflux> dataInfluxList = new ArrayList<>();
        for(NetworkMetric networkMetric : networkMetrics) {
            if (networkMetric.getData() != null) {

                GsonBuilder gsonBuilder = new GsonBuilder();
                gsonBuilder.setDateFormat(TIMESTAMP_FORMAT);
                //gsonBuilder.setLongSerializationPolicy(LongSerializationPolicy.STRING);

                DataInflux[] dataArr = gsonBuilder.create().fromJson(networkMetric.getData(), DataInflux[].class);
                dataInflux = dataArr[dataArr.length - 1];
                float[] l = dataInflux.getLatencyArr();
                if(l.length>0) dataInflux.setLatencyVal(l[0]);
                getDeltaValueByMetric();
                mapCpuFrequency();
            }
            if (networkMetric.getJobData() == null) {
                networkMetric.setJobData(new JobMetric());
            }
            setJobData(networkMetric, dataInflux);
            dataInfluxList.add(dataInflux);
        }
        return dataInfluxList;
    }

    private void mapCpuFrequency() {
        if(dataInflux.getCpuFrequency()!=null && dataInflux.getCpuFrequency().length!=0) {
            dataInflux.setCurrCpuFrequency(dataInflux.getCpuFrequency()[0]);
            dataInflux.setMinCpuFrequency(dataInflux.getCpuFrequency()[1]);
            dataInflux.setMaxCpuFrequency(dataInflux.getCpuFrequency()[2]);
        }
    }

    private void getDeltaValueByMetric() {

        dataInflux.setBytesSentDelta(bytesSentOld!=0
                ? dataInflux.getBytesSent()-bytesSentOld
                : bytesSentOld);
        bytesSentOld=dataInflux.getBytesSent();

        dataInflux.setBytesReceivedDelta(bytesReceivedOld!=0
                ? dataInflux.getBytesReceived()-bytesReceivedOld
                : bytesReceivedOld);
        bytesReceivedOld=dataInflux.getBytesReceived();

        dataInflux.setPacketsSentDelta(packetsSentOld!=0
                ? dataInflux.getPacketSent()-packetsSentOld
                : packetsSentOld);
        packetsSentOld=dataInflux.getPacketSent();

        dataInflux.setPacketsReceivedDelta(packetsReceivedOld!=0
                ? dataInflux.getPacketReceived()-packetsReceivedOld
                : packetsReceivedOld);
        packetsReceivedOld=dataInflux.getPacketReceived();

    }

    private void setJobData(NetworkMetric networkMetric, DataInflux dataInflux){
        if(dataInflux == null) dataInflux = new DataInflux();
        JobMetric jobMetric = networkMetric.getJobData();
        dataInflux.setConcurrency(jobMetric.getConcurrency());
        dataInflux.setParallelism(jobMetric.getParallelism());
        dataInflux.setPipelining(jobMetric.getPipelining());
        dataInflux.setThroughput(jobMetric.getThroughput());
        dataInflux.setJobId(jobMetric.getJobId());
    }
}
