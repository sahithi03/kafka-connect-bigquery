package com.wepay.kafka.connect.bigquery.route;

import com.wepay.kafka.connect.bigquery.utils.PartitionedTableId;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Map;

public interface TableRouter {
    /**
     * Called with all of the configuration settings passed to the connector via its
     * {@link org.apache.kafka.connect.sink.SinkConnector#start(Map)} method.
     * @param properties The configuration settings of the connector.
     */
    void configure(Map<String,String> properties);

    /**
     * Returns the table that a sinkRecord has to be written to
     * @param sinkRecord The Kafka Connect Sink Record
     * @return PartitionedTableId A TableId with separate base table name and partition information
     */
    PartitionedTableId getTable(SinkRecord sinkRecord);


}
