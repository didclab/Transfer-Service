package org.onedatashare.transferservice.odstransferservice.service.DatabaseService.metric;

import com.google.gson.Gson;
import org.onedatashare.transferservice.odstransferservice.DataRepository.NetworkMetricRepository;
import org.onedatashare.transferservice.odstransferservice.model.NetworkMetric;
import org.onedatashare.transferservice.odstransferservice.utility.DataUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
/**
 * @author deepika
 */
@Service
public class NetworkMetricServiceImpl implements NetworkMetricService {

    public static final String SINGLE_QUOTE = "'";
    Logger logger = LoggerFactory.getLogger(NetworkMetricService.class);

    @Autowired
    NetworkMetricRepository repository;

    @Autowired
    JdbcTemplate jdbcTemplate;


    @Override
    public NetworkMetric saveOrUpdate(NetworkMetric networkMetric) {
        try {
            logger.info("Saving");
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
            logger.info("sql insert: " + stringBuilder);
            jdbcTemplate.execute(stringBuilder.toString());

        }
        catch (Exception ex) {
            ex.getMessage();
        }
        return null;
    }
}
