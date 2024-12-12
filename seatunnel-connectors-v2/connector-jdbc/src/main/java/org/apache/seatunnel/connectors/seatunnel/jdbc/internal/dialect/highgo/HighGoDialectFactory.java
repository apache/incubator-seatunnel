package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.highgo;

import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PostgresDialectFactory;

import com.google.auto.service.AutoService;

/** @ Author：SimonChou @ Date：2024-12-12-11:29 @ Description：HighGoDialectFactory */
@AutoService(JdbcDialectFactory.class)
public class HighGoDialectFactory extends PostgresDialectFactory {
    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:highgo:");
    }
}
