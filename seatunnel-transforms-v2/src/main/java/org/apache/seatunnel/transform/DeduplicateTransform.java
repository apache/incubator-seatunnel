package org.apache.seatunnel.transform;

import com.google.auto.service.AutoService;
import org.apache.commons.codec.binary.Base64;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.transform.common.AbstractSeaTunnelTransform;

import java.util.List;
import java.util.StringJoiner;

@AutoService(SeaTunnelTransform.class)
public class DeduplicateTransform extends AbstractSeaTunnelTransform {
    public static final Option<List<String>> KEY_DUPLICATE_FIELDS = Options.key("duplicate_fields")
            .listType()
            .noDefaultValue()
            .withDescription("The duplicate fields for check");

    private String previousValues;

    private String[] duplicateFields;

    private int[] duplicateFieldsIndex;

    @Override
    public String getPluginName() {
        return "Deduplicate";
    }

    @Override
    protected void setConfig(Config pluginConfig) {
        CheckResult checkResult = CheckConfigUtil.checkAllExists(pluginConfig, KEY_DUPLICATE_FIELDS.key());
        if (!checkResult.isSuccess()) {
            throw new IllegalArgumentException("Failed to check config! " + checkResult.getMsg());
        }
        duplicateFields = pluginConfig.getStringList(KEY_DUPLICATE_FIELDS.key()).toArray(new String[0]);
    }

    @Override
    protected SeaTunnelRowType transformRowType(SeaTunnelRowType rowType) {
        duplicateFieldsIndex = new int[duplicateFields.length];
        for (int idx = 0; idx < duplicateFields.length; idx++) {
            String duplicateField = duplicateFields[idx];
            int duplicateFieldIndex = rowType.indexOf(duplicateField);
            if (duplicateFieldIndex == -1) {
                throw new IllegalArgumentException(String.format("Cannot find [%s] field in input row type", duplicateFieldIndex));
            }
            duplicateFieldsIndex[idx] = duplicateFieldIndex;
        }
        return rowType;
    }

    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        StringJoiner strJoiner = new StringJoiner("|");
        for (int duplicateFieldIdx : duplicateFieldsIndex) {
            Object value = inputRow.getField(duplicateFieldIdx);
            if (value == null) {
                value = "\\N";
            } else if (value instanceof byte[]) {
                Base64 base64 = new Base64();
                value = base64.encodeToString((byte[]) value);
            }
            strJoiner.add(value.toString());
        }
        String currentValues = strJoiner.toString();
        if (currentValues.equals(previousValues)) {
            inputRow = null;
        }

        previousValues = currentValues;

        return inputRow;
    }
}
