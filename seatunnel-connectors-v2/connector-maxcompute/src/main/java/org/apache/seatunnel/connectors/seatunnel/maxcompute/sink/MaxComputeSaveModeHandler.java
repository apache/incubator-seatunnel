package org.apache.seatunnel.connectors.seatunnel.maxcompute.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.DefaultSaveModeHandler;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.catalog.MaxComputeCatalog;

import org.apache.commons.lang3.StringUtils;

import com.aliyun.odps.PartitionSpec;

import static org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeConfig.PARTITION_SPEC;

public class MaxComputeSaveModeHandler extends DefaultSaveModeHandler {

    private final ReadonlyConfig readonlyConfig;

    public MaxComputeSaveModeHandler(
            SchemaSaveMode schemaSaveMode,
            DataSaveMode dataSaveMode,
            Catalog catalog,
            CatalogTable catalogTable,
            String customSql,
            ReadonlyConfig readonlyConfig) {
        super(schemaSaveMode, dataSaveMode, catalog, catalogTable, customSql);
        this.readonlyConfig = readonlyConfig;
    }

    @Override
    protected void createSchemaWhenNotExist() {
        super.createSchemaWhenNotExist();
        if (StringUtils.isNotEmpty(readonlyConfig.get(PARTITION_SPEC))) {
            ((MaxComputeCatalog) catalog)
                    .createPartition(
                            tablePath, new PartitionSpec(readonlyConfig.get(PARTITION_SPEC)));
        }
    }

    @Override
    protected void recreateSchema() {
        super.recreateSchema();
        if (StringUtils.isNotEmpty(readonlyConfig.get(PARTITION_SPEC))) {
            ((MaxComputeCatalog) catalog)
                    .createPartition(
                            tablePath, new PartitionSpec(readonlyConfig.get(PARTITION_SPEC)));
        }
    }
}
