package com.wepay.kafka.connect.bigquery.route;

import com.google.cloud.bigquery.TableId;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig;
import com.wepay.kafka.connect.bigquery.utils.PartitionedTableId;
import com.wepay.kafka.connect.bigquery.utils.TopicToTableResolver;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Map;

/**
 * A class for fetching the table a sinkRecord should be written to based on the topic regex
 */

public class RegexTableRouter implements TableRouter{

    private BigQuerySinkTaskConfig config;
    private Map<String, TableId> topicsToBaseTableIds;
    private boolean usePartitionDecorator;
    private boolean useMessageTimeDatePartitioning;

    @Override
    public void configure(Map<String, String> properties) {
        config = new BigQuerySinkTaskConfig(properties);
        topicsToBaseTableIds = TopicToTableResolver.getTopicsToTables(config);
        usePartitionDecorator =
                config.getBoolean(config.BIGQUERY_PARTITION_DECORATOR_CONFIG);
        useMessageTimeDatePartitioning =
                config.getBoolean(config.BIGQUERY_MESSAGE_TIME_PARTITIONING_CONFIG);
    }

    @Override
    public PartitionedTableId getTable(SinkRecord sinkRecord) {

        // Dynamically update topicToBaseTableIds mapping. topicToBaseTableIds was used to be
        // constructed when connector starts hence new topic configuration needed connector to restart.
        // Dynamic update shall not require connector restart and shall compute table id in runtime.

        if (!topicsToBaseTableIds.containsKey(sinkRecord.topic())) {
            TopicToTableResolver.updateTopicToTable(config, sinkRecord.topic(), topicsToBaseTableIds);
        }

        TableId baseTableId = topicsToBaseTableIds.get(sinkRecord.topic());

        PartitionedTableId.Builder builder = new PartitionedTableId.Builder(baseTableId);
        if (usePartitionDecorator) {
            if (useMessageTimeDatePartitioning) {
                if (sinkRecord.timestampType() == TimestampType.NO_TIMESTAMP_TYPE) {
                    throw new ConnectException(
                            "Message has no timestamp type, cannot use message timestamp to partition.");
                }
                builder.setDayPartition(sinkRecord.timestamp());
            } else {
                builder.setDayPartitionForNow();
            }
        }
        return builder.build();
    }
}

