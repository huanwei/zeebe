/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.it.clustering;

import static io.zeebe.test.util.TestUtil.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.broker.Broker;
import io.zeebe.broker.system.configuration.BrokerCfg;
import io.zeebe.broker.system.configuration.DataCfg;
import io.zeebe.broker.system.configuration.ExporterCfg;
import io.zeebe.exporter.api.Exporter;
import io.zeebe.exporter.api.context.Controller;
import io.zeebe.protocol.record.Record;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.util.unit.DataSize;

public class SingleBrokerDataDeletionTest {

  private static final Duration SNAPSHOT_PERIOD = Duration.ofMinutes(5);
  private static final int SEGMENT_COUNT = 5;

  @Rule
  public final ClusteringRule clusteringRule =
      new ClusteringRule(1, 1, 1, this::configureCustomExporter);

  private final AtomicLong writtenRecords = new AtomicLong(0);

  private void configureCustomExporter(final BrokerCfg brokerCfg) {
    final DataCfg data = brokerCfg.getData();
    data.setSnapshotPeriod(SNAPSHOT_PERIOD);
    data.setLogSegmentSize(DataSize.ofKilobytes(8));
    data.setLogIndexDensity(5);
    brokerCfg.getNetwork().setMaxMessageSize(DataSize.ofKilobytes(8));

    final ExporterCfg exporterCfg = new ExporterCfg();
    exporterCfg.setClassName(ControllableExporter.class.getName());
    brokerCfg.setExporters(Collections.singletonMap("snapshot-test-exporter", exporterCfg));
  }

  @Before
  public void init() {
    ControllableExporter.NOT_EXPORTED_RECORDS.clear();
    ControllableExporter.updatePosition(true);
    ControllableExporter.EXPORTED_RECORDS.set(0);

    writtenRecords.set(0);
  }

  @Test
  public void shouldNotCompactNotExportedEvents() {
    // given
    final Broker broker = clusteringRule.getBroker(0);

    final var logstream = clusteringRule.getLogStream(1);
    final var reader = logstream.newLogStreamReader().join();

    while (getSegmentsCount(broker) <= SEGMENT_COUNT) {
      writeToLog();
    }

    waitUntil(
        () -> ControllableExporter.EXPORTED_RECORDS.get() >= writtenRecords.get(),
        () ->
            String.format(
                "Expected all written records to be exported (%d/%d)",
                ControllableExporter.EXPORTED_RECORDS.get(), writtenRecords.get()));

    // when
    ControllableExporter.updatePosition(false);

    // write more events
    while (getSegmentsCount(broker) <= SEGMENT_COUNT * 2) {
      writeToLog();
    }

    waitUntil(
        () -> ControllableExporter.EXPORTED_RECORDS.get() >= writtenRecords.get(),
        () ->
            String.format(
                "Expected all written records to be exported (%d/%d)",
                ControllableExporter.EXPORTED_RECORDS.get(), writtenRecords.get()));

    // increase snapshot interval and wait
    clusteringRule.getClock().addTime(SNAPSHOT_PERIOD);
    final var firstSnapshot = clusteringRule.waitForSnapshotAtBroker(broker);

    // then
    final var firstNonExportedPosition =
        ControllableExporter.NOT_EXPORTED_RECORDS.get(0).getPosition();

    waitUntil(
        () -> {
          try {
            // may fail if the compaction is not completed yet
            reader.seek(firstNonExportedPosition);
            return reader.hasNext();

          } catch (final Exception ignore) {
            return false;
          }
        });

    assertThat(reader.next().getPosition())
        .describedAs("Expected first non-exported record to be present in the log but not found.")
        .isEqualTo(firstNonExportedPosition);

    // when
    final var segmentsBeforeSnapshot = getSegmentsCount(broker);
    ControllableExporter.updatePosition(true);

    writeToLog();

    waitUntil(
        () -> ControllableExporter.EXPORTED_RECORDS.get() >= writtenRecords.get(),
        () ->
            String.format(
                "Expected all written records to be exported (%d/%d)",
                ControllableExporter.EXPORTED_RECORDS.get(), writtenRecords.get()));

    // increase snapshot interval and wait
    clusteringRule.getClock().addTime(SNAPSHOT_PERIOD);
    clusteringRule.waitForNewSnapshotAtBroker(broker, firstSnapshot);

    // then
    waitUntil(
        () -> getSegmentsCount(broker) < segmentsBeforeSnapshot,
        () ->
            String.format(
                "Expected segment count to be less than %d after a snapshot is taken but was %d",
                segmentsBeforeSnapshot, getSegmentsCount(broker)));
  }

  private void writeToLog() {
    clusteringRule
        .getClient()
        .newPublishMessageCommand()
        .messageName("msg")
        .correlationKey("key")
        .send()
        .join();
  }

  private int getSegmentsCount(final Broker broker) {
    return getSegments(broker).size();
  }

  private Collection<Path> getSegments(final Broker broker) {
    try {
      return Files.list(clusteringRule.getSegmentsDirectory(broker))
          .filter(path -> path.toString().endsWith(".log"))
          .collect(Collectors.toList());
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static class ControllableExporter implements Exporter {
    static final List<Record> NOT_EXPORTED_RECORDS = new CopyOnWriteArrayList<>();
    static volatile boolean shouldExport = true;

    static final AtomicLong EXPORTED_RECORDS = new AtomicLong(0);

    private Controller controller;

    static void updatePosition(final boolean flag) {
      shouldExport = flag;
    }

    @Override
    public void open(final Controller controller) {
      this.controller = controller;
    }

    @Override
    public void export(final Record record) {
      if (shouldExport) {
        controller.updateLastExportedRecordPosition(record.getPosition());
      } else {
        NOT_EXPORTED_RECORDS.add(record);
      }

      EXPORTED_RECORDS.incrementAndGet();
    }
  }
}
