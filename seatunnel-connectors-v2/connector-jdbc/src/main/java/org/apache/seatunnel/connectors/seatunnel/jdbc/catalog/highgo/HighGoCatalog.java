package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.highgo;

import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.psql.PostgresCatalog;
import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;

import java.sql.Connection;

/**
 * @ Author：SimonChou
 * @ Date：2024-12-12-11:17
 * @ Description：HighGoCatalog
 */
public class HighGoCatalog extends PostgresCatalog {

    public HighGoCatalog(String catalogName, String username, String pwd, JdbcUrlUtil.UrlInfo urlInfo, String defaultSchema) {
        super(catalogName, username, pwd, urlInfo, defaultSchema);
    }

    @VisibleForTesting
    public void setConnection(String url, Connection connection) {
        this.connectionMap.put(url, connection);
    }
}

