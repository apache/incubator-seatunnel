package org.apache.seatunnel.engine.server.metrics;

import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.runtime.ExecutionMode;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.rest.RestConstant;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.hazelcast.instance.impl.HazelcastInstanceImpl;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;

public class MetricsApiTest {

    private static HazelcastInstanceImpl instance;

    @BeforeAll
    public static void before() {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.getEngineConfig().getTelemetryConfig().getMetric().setEnabled(true);
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(true);
        seaTunnelConfig.getEngineConfig().getHttpConfig().setPort(8080);
        seaTunnelConfig.getEngineConfig().setMode(ExecutionMode.LOCAL);
        instance = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);
    }

    @Test
    public void metricsApiTest() {
        given().get("http://localhost:8080" + RestConstant.REST_URL_METRICS)
                .then()
                .statusCode(200)
                .body(containsString("process_start_time_seconds"));
    }

    @AfterAll
    public static void after() {
        if (instance != null) {
            instance.shutdown();
        }
    }
}
