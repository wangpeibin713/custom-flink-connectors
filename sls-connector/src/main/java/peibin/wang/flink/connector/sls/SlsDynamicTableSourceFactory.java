/**
 * wangpeibin713@gmail.com Copyright (c) 2004-2021 All Rights Reserved.
 */
package peibin.wang.flink.connector.sls;

import com.aliyun.openservices.log.flink.model.LogDeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DecodingFormatFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author wangpeibin713@gmail.com
 * @version : SlsDynamicTableSourceFactory.java 2021年01月11日 13:54 wangpeibin713@gmail.com Exp $
 */
public class SlsDynamicTableSourceFactory implements DynamicTableSourceFactory {

    public static final ConfigOption<String> PROJECT = ConfigOptions.key("project").stringType().noDefaultValue();
    public static final ConfigOption<String> ACCESS_ID = ConfigOptions.key("access.id").stringType().noDefaultValue();
    public static final ConfigOption<String> ACCESS_KEY = ConfigOptions.key("access.key").stringType().noDefaultValue();
    public static final ConfigOption<String> ENDPOINT = ConfigOptions.key("endpoint").stringType().noDefaultValue();
    public static final ConfigOption<String> LOGSTORE               = ConfigOptions.key("logstore").stringType().noDefaultValue();
    public static final ConfigOption<String> CONSUMER_BEGINPOSITION = ConfigOptions.key("consumer.beginposition").stringType().noDefaultValue();
    public static final ConfigOption<String> FORMAT                 = ConfigOptions.key("format").stringType().noDefaultValue();

    public SlsDynamicTableSourceFactory() {}
    @Override
    public String factoryIdentifier() {
        return "sls";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet();
        options.add(PROJECT);
        options.add(ACCESS_ID);
        options.add(ACCESS_KEY);
        options.add(ENDPOINT);
        options.add(LOGSTORE);
        options.add(CONSUMER_BEGINPOSITION);
        options.add(FORMAT);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet();
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        // either implement your custom validation logic here ...
        // or use the provided helper utility
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        // discover a suitable decoding format
        final DecodingFormat<LogDeserializationSchema<RowData>> decodingFormat = helper.discoverDecodingFormat(
                DecodingFormatFactory.class,
                FactoryUtil.FORMAT);
        //final DecodingFormat<LogDeserializationSchema<RowData>> decodingFormat = new SlsFormat();

        // validate all options
        helper.validate();
        TableSchema schema = context.getCatalogTable().getSchema();
        // get the validated options
        final ReadableConfig options = helper.getOptions();
        String project = options.get(PROJECT);
        String accessId = options.get(ACCESS_ID);
        String accessKey = options.get(ACCESS_KEY);
        String endpoint = options.get(ENDPOINT);
        String logstore = options.get(LOGSTORE);
        String consumerBeginposition = options.get(CONSUMER_BEGINPOSITION);

        // derive the produced data type (excluding computed columns) from the catalog table
        final DataType producedDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();
        return new SlsDynamicTableSource(project,accessId,accessKey,endpoint,logstore,consumerBeginposition,decodingFormat, producedDataType,schema);
    }
}
