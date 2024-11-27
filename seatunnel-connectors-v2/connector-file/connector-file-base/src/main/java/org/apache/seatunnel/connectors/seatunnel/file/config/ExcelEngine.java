package org.apache.seatunnel.connectors.seatunnel.file.config;

import java.io.Serializable;

public enum ExcelEngine implements Serializable {
    POI("POI"),
    EASY_EXCEL("EasyExcel");

    private final String excelEngineName;

    ExcelEngine(String excelEngineName) {
        this.excelEngineName = excelEngineName;
    }

    public String getExcelEngineName() {
        return excelEngineName;
    }
}
