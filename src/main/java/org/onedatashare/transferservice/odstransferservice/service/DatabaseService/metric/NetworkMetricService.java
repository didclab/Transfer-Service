package org.onedatashare.transferservice.odstransferservice.service.DatabaseService.metric;

import org.onedatashare.transferservice.odstransferservice.model.NetworkMetric;
import org.onedatashare.transferservice.odstransferservice.model.metrics.DataInflux;
import org.springframework.scripting.bsh.BshScriptUtils;

import java.util.List;

/**
 * @author deepika
 */
public interface NetworkMetricService {
    NetworkMetric saveOrUpdate(NetworkMetric networkMetric);
    NetworkMetric readFile();
    void executeScript() throws Exception;
    DataInflux mapData(NetworkMetric networkMetric);
}
