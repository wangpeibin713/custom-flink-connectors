/**
 * wangpeibin713@gmail.com Copyright (c) 2004-2021 All Rights Reserved.
 */
package peibin.wang.flink.connector.sls;

import com.aliyun.openservices.log.common.FastLog;
import com.aliyun.openservices.log.common.FastLogContent;
import com.aliyun.openservices.log.common.LogGroupData;
import com.aliyun.openservices.log.flink.model.LogDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author wangpeibin713@gmail.com
 * @version : SlsDeserializer.java 2021年01月11日 13:54 wangpeibin713@gmail.com Exp $
 */
public class SlsDeserializer implements LogDeserializationSchema<RowData> {
    private final DataType                                  dataType;
    private final DynamicTableSource.DataStructureConverter converter;
    private final TypeInformation<RowData>                  producedTypeInfo;
    private final Map<String, LogicalType>                  fields = new HashMap<>();

    public SlsDeserializer(DataType dataType, DynamicTableSource.DataStructureConverter converter,
                           TypeInformation<RowData> producedTypeInfo) {
        this.dataType = dataType;
        this.converter = converter;
        this.producedTypeInfo = producedTypeInfo;
        RowType rowType = (RowType) dataType.getLogicalType();
        for (RowType.RowField field : rowType.getFields()) {
            fields.put(field.getName(), field.getType());
        }

    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedTypeInfo;
    }

    @Override
    public RowData deserialize(List<LogGroupData> logGroups) {
        throw  new UnsupportedOperationException();
    }
    @Override
    public List<RowData> deserializeList(List<LogGroupData> logGroups) {
        List<RowData> result = new ArrayList<>();
        logGroups.stream()
                .map(LogGroupData::GetFastLogGroup)
                .forEach(x -> {
                    for (int i = 0; i < x.getLogsCount(); i++) {
                        RowData rowData = log2Row(x.getLogs(i));
                        result.add(rowData);
                    }
                });
        return result;

    }

    private RowData log2Row(FastLog fastLog) {
        int count = fastLog.getContentsCount();
        final RowKind kind = RowKind.valueOf("INSERT");
        final Row row = new Row(kind, fields.size());
        Map<String, String> data = new HashMap<>();
        for (int cIdx = 0; cIdx < count; ++cIdx) {
            FastLogContent content = fastLog.getContents(cIdx);
            data.put(content.getKey(),content.getValue());
        }

        RowType rowType = (RowType) dataType.getLogicalType();
        List<RowType.RowField> fields = rowType.getFields();
        for (int i = 0; i < fields.size(); i++) {
            Object o = parseValue(data.get(fields.get(i).getName()), fields.get(i).getType());
            row.setField(i, o );
        }
        return (RowData) converter.toInternal(row);
    }

    private Object parseValue(FastLogContent content) {
        LogicalType logicalType = fields.get(content.getKey());

        //return LogicalTypeUtils.toInternalConversionClass(logicalType).cast(content.getValue());
        return parseValue(content.getValue(),logicalType);
    }

    private Object parseValue(String value, LogicalType logicalType) {
        if (logicalType instanceof IntType) {
            return Integer.parseInt(value);
        }
        if (logicalType instanceof BigIntType) {
            return Long.parseLong(value);
        }

        if (logicalType instanceof FloatType) {
            return Float.parseFloat(value);
        }

        if (logicalType instanceof DoubleType) {
            return Double.parseDouble(value);
        }
        return value;
    }
}
