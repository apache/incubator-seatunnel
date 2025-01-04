package org.apache.seatunnel.connectors.seatunnel.file.split;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSourceSplit;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSourceSplitEnumerator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class FileSourceSplitEnumeratorTest {

    @Test
    void assignSplitRoundTest() {
        List<String> filePaths = new ArrayList<>();
        int fileSize = 10;
        int parallelism = 4;

        for (int i = 0; i < fileSize; i++) {
            filePaths.add("file" + i + ".txt");
        }

        Map<Integer, List<FileSourceSplit>> assignSplitMap = new HashMap<>();

        SourceSplitEnumerator.Context<FileSourceSplit> context =
                new SourceSplitEnumerator.Context<FileSourceSplit>() {
                    @Override
                    public int currentParallelism() {
                        return parallelism;
                    }

                    @Override
                    public Set<Integer> registeredReaders() {
                        return null;
                    }

                    @Override
                    public void assignSplit(int subtaskId, List<FileSourceSplit> splits) {
                        assignSplitMap.put(subtaskId, splits);
                    }

                    @Override
                    public void signalNoMoreSplits(int subtask) {}

                    @Override
                    public void sendEventToSourceReader(int subtaskId, SourceEvent event) {}

                    @Override
                    public MetricsContext getMetricsContext() {
                        return null;
                    }

                    @Override
                    public EventListener getEventListener() {
                        return null;
                    }
                };

        FileSourceSplitEnumerator fileSourceSplitEnumerator =
                new FileSourceSplitEnumerator(context, filePaths);
        fileSourceSplitEnumerator.open();

        fileSourceSplitEnumerator.run();

        // check all files are assigned
        Assertions.assertEquals(
                fileSize, assignSplitMap.values().stream().mapToInt(List::size).sum());

        Set<FileSourceSplit> valueSet =
                assignSplitMap.values().stream().flatMap(List::stream).collect(Collectors.toSet());

        // check no duplicated assigned split
        Assertions.assertEquals(valueSet.size(), fileSize);

        // check file allocation balance
        for (int i = 1; i < parallelism; i++) {
            Assertions.assertTrue(
                    Math.abs(assignSplitMap.get(i).size() - assignSplitMap.get(i - 1).size()) <= 1,
                    "The number of files assigned to adjacent subtasks is more than 1.");
        }
    }
}
