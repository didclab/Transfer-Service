package org.onedatashare.transferservice.odstransferservice.cron.metric;

import com.google.api.client.json.Json;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.vladmihalcea.hibernate.type.json.JsonType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.Type;

import javax.persistence.*;

/**
 * @author deepika
 */
@Entity
@Table(name = "network_metric")
//@Table(name = "job_info_3") //todo - replace
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Data
public class NetworkMetric {

    @Id
    private String id;
    
}
