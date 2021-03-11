package peibin.wang.flink.connector.sls; /**
 * wangpeibin713@gmail.com Copyright (c) 2004-2021 All Rights Reserved.
 */

import com.aliyun.openservices.log.flink.model.LogDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;


/**
 *
 * @author wangpeibin713@gmail.com
 * @version : peibin.wang.flink.connector.sls.SlsFormat.java 2021年01月11日 13:53 wangpeibin713@gmail.com Exp $
 */
public class SlsFormat implements DecodingFormat<LogDeserializationSchema<RowData>> {

    @Override
    public LogDeserializationSchema<RowData> createRuntimeDecoder(DynamicTableSource.Context context, DataType dataType) {
        // create type information for the DeserializationSchema
        //创建反序列化schema
        final TypeInformation<RowData> producedTypeInfo =  context.createTypeInformation(
                dataType);

        // most of the code in DeserializationSchema will not work on internal data structures
        // create a converter for conversion at the end
        final DynamicTableSource.DataStructureConverter converter = context.createDataStructureConverter(dataType);

        // use logical types during runtime for parsing
        //final List<LogicalType> parsingTypes = dataType.getLogicalType().getChildren();

        // create runtime class
        return new SlsDeserializer(dataType, converter, producedTypeInfo);
    }

    @Override
    public ChangelogMode getChangelogMode() {
        // define that this format can produce INSERT and DELETE rows
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .build();
    }
}
