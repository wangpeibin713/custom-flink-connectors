/**
 * wangpeibin713@gmail.com Copyright (c) 2004-2021 All Rights Reserved.
 */
package peibin.wang.flink.connector.sls;

import com.aliyun.openservices.log.flink.model.LogDeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DecodingFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author wangpeibin713@gmail.com
 * @version : SlsFormatFactory.java 2021年01月11日 13:53 wangpeibin713@gmail.com Exp $
 */
public class SlsFormatFactory implements DecodingFormatFactory<LogDeserializationSchema<RowData>> {

    @Override
    public DecodingFormat<LogDeserializationSchema<RowData>> createDecodingFormat(DynamicTableFactory.Context context, ReadableConfig readableConfig) {
        // either implement your custom validation logic here ...
        // or use the provided helper method
        FactoryUtil.validateFactoryOptions(this, readableConfig);

        // create and return the format
        return new SlsFormat();
    }

    @Override
    public String factoryIdentifier() {
        return "slsFormat";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        return options;
    }
}
