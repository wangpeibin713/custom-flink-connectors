/**
 * wangpeibin713@gmail.com Copyright (c) 2004-2021 All Rights Reserved.
 */
package peibin.wang.flink.connector.sls;

import com.aliyun.openservices.log.flink.ConfigConstants;
import com.aliyun.openservices.log.flink.FlinkLogConsumer;
import com.aliyun.openservices.log.flink.model.LogDeserializationSchema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.apache.flink.table.connector.source.SourceFunctionProvider;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 *
 * @author wangpeibin713@gmail.com
 * @version : SlsDynamicTableSource.java 2021年01月11日 13:55 wangpeibin713@gmail.com Exp $
 */
public class SlsDynamicTableSource implements ScanTableSource {
    private String project;
    private String accessId;
    private String accessKey;
    private String endpoint;
    private String logstore;
    private String                                            consumerBeginposition;
    private DecodingFormat<LogDeserializationSchema<RowData>> decodingFormat;
    private DataType                                          producedDataType;
    private TableSchema                                       schema;


    public SlsDynamicTableSource(String project, String accessId, String accessKey, String endpoint, String logstore, String consumerBeginposition,
                                 DecodingFormat<LogDeserializationSchema<RowData>> decodingFormat, DataType producedDataType,
                                 TableSchema schema
    ) {
        this.project = project;
        this.accessId = accessId;
        this.accessKey = accessKey;
        this.endpoint = endpoint;
        this.logstore = logstore;
        this.consumerBeginposition = consumerBeginposition;
        this.decodingFormat = decodingFormat;
        this.producedDataType = producedDataType;
        this.schema = schema;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .build();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        // create runtime classes that are shipped to the cluster

        final LogDeserializationSchema<RowData> deserializer = decodingFormat.createRuntimeDecoder(
                scanContext,
                producedDataType);

        //逗号切割logstores名字
        List<String> topics = Arrays.asList(this.logstore.split(","));
        Properties slsProperties = new Properties();
        // 设置访问日志服务的域名
        slsProperties.put(ConfigConstants.LOG_ENDPOINT, this.endpoint);
        // 设置访问ak
        slsProperties.put(ConfigConstants.LOG_ACCESSSKEYID, this.accessId);
        slsProperties.put(ConfigConstants.LOG_ACCESSKEY, this.accessKey);
        // 设置消费日志服务起始位置
        /**
         * begin_cursor, end_cursor, unix timestamp or consumer_from_checkpoint
         */
        slsProperties.put(ConfigConstants.LOG_CONSUMER_BEGIN_POSITION, this.consumerBeginposition);
        //        /**
        //         * 消费组名
        //         */
        //        slsProperties.put(ConfigConstants.LOG_CONSUMERGROUP, "flink-consumer-test");
        //        slsProperties.put(ConfigConstants.LOG_FETCH_DATA_INTERVAL_MILLIS, 3000);
        //        slsProperties.put(ConfigConstants.LOG_MAX_NUMBER_PER_FETCH, 10);
        //        /**
        //         * DISABLED---Never commit checkpoint to remote server.
        //         * ON_CHECKPOINTS---Commit checkpoint only when Flink creating checkpoint, which means Flink
        //         *                  checkpointing must be enabled.
        //         * PERIODIC---Auto commit checkpoint periodic.
        //         */
        //        slsProperties.put(ConfigConstants.LOG_CHECKPOINT_MODE, CheckpointMode.ON_CHECKPOINTS.name());
        //        /**
        //         * 应该是如果ConfigConstants.LOG_CHECKPOINT_MODE设置了CheckpointMode.PERIODIC,则可以设置自动提交间隔
        //         * slsProperties.put(ConfigConstants.LOG_COMMIT_INTERVAL_MILLIS, "10000");
        //         */

        FlinkLogConsumer<RowData> flinkLogConsumer = new FlinkLogConsumer<>(project, topics, (LogDeserializationSchema) deserializer, slsProperties);
        return SourceFunctionProvider.of(flinkLogConsumer, false);
    }

    @Override
    public DynamicTableSource copy() {
        return new SlsDynamicTableSource(project,accessId,accessKey,endpoint,logstore,consumerBeginposition,null, producedDataType,schema);
    }

    @Override
    public String asSummaryString() {
        return "sls Table Source";
    }
}
