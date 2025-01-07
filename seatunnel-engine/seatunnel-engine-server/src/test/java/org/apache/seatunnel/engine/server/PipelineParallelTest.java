/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.server;

import com.hazelcast.map.IMap;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;

public class PipelineParallelTest
        extends AbstractSeaTunnelServerTest {
    public static String CONF_PATH =
            "fakesource_to_console_pipeline_parallel.conf";
    public static String CONF_PATH2 =
            "fakesource_to_console_pipeline_parallel2.conf";


    @Test
    public void testPipelineParallelStart() {
        long jobId = System.currentTimeMillis();
        // ipeline_parallelism = 3
        // pipeline_wait_seconds = 7
        startJob(jobId, CONF_PATH, false);

        AtomicReference<List<Long>> list = new AtomicReference<>(new ArrayList<>());
        await().atMost(180000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            long count = list.get().stream().filter(a -> a > 0).count();
                            if (count < 9) {
                                IMap<Object, Long[]> map = nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_STATE_TIMESTAMPS);
                                if (map.get(jobId) != null) {
                                    list.set(new ArrayList<>());
                                    for (int i = 1; i <= 10; i++) {
                                        PipelineLocation pipelineLocation = new PipelineLocation(jobId, i);
                                        long l = Optional.ofNullable(map.get(pipelineLocation)).map(a -> a[PipelineStatus.SCHEDULED.ordinal()]).orElse(0L);
                                        list.get().add(l);
                                    }
                                    list.get().sort((a, b) -> (int) (a - b));
                                    count = list.get().stream().filter(a -> a > 0).count();
                                }
                            }
                            Assertions.assertEquals(
                                    server.getCoordinatorService().getJobStatus(jobId),
                                    JobStatus.FINISHED);
                            Assertions.assertTrue(count >= 9);
                            Assertions.assertTrue(list.get().get(6) - list.get().get(3) > 7000);
                            Assertions.assertTrue(list.get().get(9) - list.get().get(6) > 7000);
                            Assertions.assertTrue(list.get().get(9) - list.get().get(8) < 7000);
                            System.out.println("parallel time: " + list);
                        });

    }

    @Test
    public void testPipelineParallelStart2() {
        long jobId = System.currentTimeMillis();
        // pipeline_parallelism = 4
        // pipeline_wait_seconds = 10
        startJob(jobId, CONF_PATH2, false);

        AtomicReference<List<Long>> list = new AtomicReference<>(new ArrayList<>());
        await().atMost(240000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            long count = list.get().stream().filter(a -> a > 0).count();
                            if (count < 8) {
                                IMap<Object, Long[]> map = nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_STATE_TIMESTAMPS);
                                if (map.get(jobId) != null) {
                                    list.set(new ArrayList<>());
                                    for (int i = 1; i <= 10; i++) {
                                        PipelineLocation pipelineLocation = new PipelineLocation(jobId, i);
                                        long l = Optional.ofNullable(map.get(pipelineLocation)).map(a -> a[PipelineStatus.SCHEDULED.ordinal()]).orElse(0L);
                                        list.get().add(l);
                                    }
                                    list.get().sort((a, b) -> (int) (a - b));
                                    count = list.get().stream().filter(a -> a > 0).count();
                                }
                            }
                            Assertions.assertEquals(
                                    server.getCoordinatorService().getJobStatus(jobId),
                                    JobStatus.FINISHED);

                            Assertions.assertTrue(count >= 8);
                            Assertions.assertTrue(list.get().get(5) - list.get().get(1) > 10000);
                            Assertions.assertTrue(list.get().get(9) - list.get().get(5) > 10000);
                            Assertions.assertTrue(list.get().get(9) - list.get().get(8) < 10000);
                            System.out.println("parallel2 time: " + list);
                        });

    }

}
