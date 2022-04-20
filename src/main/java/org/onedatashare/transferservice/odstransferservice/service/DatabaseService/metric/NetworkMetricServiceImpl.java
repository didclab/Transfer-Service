package org.onedatashare.transferservice.odstransferservice.service.DatabaseService.metric;

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
    public NetworkMetric readFile() {
        Date startTime = null;
        Date endTime = null;

        File inputFile = new File(ODSConstants.PMeterConstants.PMETER_REPORT_PATH);
        File tempFile = new File(ODSConstants.PMeterConstants.PMETER_TEMP_REPORT);

        NetworkMetric networkMetric = new NetworkMetric();

        try(Reader r = new InputStreamReader(new FileInputStream(inputFile))
        ) {
            tempFile.createNewFile();
            JsonStreamParser p = new JsonStreamParser(r);
            List<Map<?, ?>> metricList = new ArrayList<>();
            while (p.hasNext()) {
                JsonElement metric = p.next();
                if (metric.isJsonObject()) {
                    Map<?, ?> map = new Gson().fromJson(metric, Map.class);
                    startTime = DataUtil.getDate((String)map.get("start_time"));
                    endTime = DataUtil.getDate((String)map.get("end_time"));
                    metricList.add(map);
                }
            }
            networkMetric.setData(new Gson().toJson(metricList));
            networkMetric.setStartTime(startTime);
            networkMetric.setEndTime(endTime);

        }catch (IOException | ParseException e){
            LOG.error("Exception occurred while reading file",e);
        }
        inputFile.delete();
        tempFile.renameTo(inputFile);

        return networkMetric;
    }

    @Override
    public void executeScript() throws Exception {
        //python3 src/pmeter/pmeter_cli.py measure en0 --user jgoldverg@gmail.com --length 1s --measure 10 -KNS
        CommandLine cmdLine = CommandLine.parse(
                String.format("python3 %s " + MEASURE +" %s --user %s --length %s %s",
                        SCRIPT_PATH, cmdLineOptions.getNetworkInterface(), cmdLineOptions.getUser(),
                        cmdLineOptions.getLength(), cmdLineOptions.getOptions()));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PumpStreamHandler streamHandler = new PumpStreamHandler(outputStream);

        DefaultExecutor executor = new DefaultExecutor();

        ExecuteWatchdog watchDog = new ExecuteWatchdog(ExecuteWatchdog.INFINITE_TIMEOUT);
        executor.setWatchdog(watchDog);
        executor.setStreamHandler(streamHandler);
        executor.execute(cmdLine);
    }

    @Override
    public DataInflux mapData(NetworkMetric networkMetric) {
        if(networkMetric.getData()!=null) {

            GsonBuilder gsonBuilder = new GsonBuilder();
            gsonBuilder.setDateFormat(TIMESTAMP_FORMAT);
            //gsonBuilder.setLongSerializationPolicy(LongSerializationPolicy.STRING);

            DataInflux[] dataArr = gsonBuilder.create().fromJson(networkMetric.getData(), DataInflux[].class);
            dataInflux = dataArr[dataArr.length-1];
            setJobData(networkMetric, dataInflux);
            float[] l = dataInflux.getLatencyArr();
            dataInflux.setLatencyVal(l[0]);
            getDeltaValueByMetric();
            mapCpuFrequency();
        }

        return dataInflux;
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
        JobMetric jobMetric = networkMetric.getJobData();
        dataInflux.setConcurrency(jobMetric.getConcurrency());
        dataInflux.setParallelism(jobMetric.getParallelism());
        dataInflux.setPipelining(jobMetric.getPipelining());
        dataInflux.setThroughput(jobMetric.getThroughput());
        dataInflux.setJobId(jobMetric.getJobId());
    }
}
