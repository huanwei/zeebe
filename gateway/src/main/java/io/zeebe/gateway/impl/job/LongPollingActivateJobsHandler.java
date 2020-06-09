/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.gateway.impl.job;

import static io.zeebe.util.sched.clock.ActorClock.currentTimeMillis;

import io.grpc.stub.StreamObserver;
import io.zeebe.gateway.Loggers;
import io.zeebe.gateway.cmd.BrokerErrorException;
import io.zeebe.gateway.impl.broker.BrokerClient;
import io.zeebe.gateway.impl.broker.cluster.BrokerClusterState;
import io.zeebe.gateway.impl.broker.response.BrokerError;
import io.zeebe.gateway.metrics.LongPollingMetrics;
import io.zeebe.gateway.protocol.GatewayOuterClass.ActivateJobsRequest;
import io.zeebe.gateway.protocol.GatewayOuterClass.ActivateJobsResponse;
import io.zeebe.protocol.record.ErrorCode;
import io.zeebe.util.sched.Actor;
import io.zeebe.util.sched.ScheduledTimer;
import io.zeebe.util.sched.clock.ActorClock;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;

public final class LongPollingActivateJobsHandler extends Actor {

  private static final String JOBS_AVAILABLE_TOPIC = "jobsAvailable";
  private static final Logger LOG = Loggers.GATEWAY_LOGGER;

  private final ActivateJobsHandler activateJobsHandler;
  private final BrokerClient brokerClient;

  // jobType -> state
  private final Map<String, InFlightLongPollingActivateJobsRequestsState> jobTypeState =
      new HashMap<>();
  private final Duration longPollingTimeout;
  private final long probeTimeoutMillis;
  private final int failedAttemptThreshold;

  private final LongPollingMetrics metrics;

  private LongPollingActivateJobsHandler(
      final BrokerClient brokerClient,
      final long longPollingTimeout,
      final long probeTimeoutMillis,
      final int failedAttemptThreshold) {
    this.brokerClient = brokerClient;
    this.activateJobsHandler = new ActivateJobsHandler(brokerClient);
    this.longPollingTimeout = Duration.ofMillis(longPollingTimeout);
    this.probeTimeoutMillis = probeTimeoutMillis;
    this.failedAttemptThreshold = failedAttemptThreshold;
    metrics = new LongPollingMetrics();
  }

  @Override
  public String getName() {
    return "GatewayLongPollingJobHandler";
  }

  @Override
  protected void onActorStarted() {
    brokerClient.subscribeJobAvailableNotification(JOBS_AVAILABLE_TOPIC, this::onNotification);
    actor.runAtFixedRate(Duration.ofMillis(probeTimeoutMillis), this::probe);
  }

  public void activateJobs(
      final ActivateJobsRequest request,
      final StreamObserver<ActivateJobsResponse> responseObserver) {
    final LongPollingActivateJobsRequest longPollingRequest =
        new LongPollingActivateJobsRequest(request, responseObserver);
    activateJobs(longPollingRequest);
  }

  public void activateJobs(final LongPollingActivateJobsRequest request) {
    actor.run(
        () -> {
          final InFlightLongPollingActivateJobsRequestsState state =
              getJobTypeState(request.getType());

          if (state.getActiveRequest() == null
              && (state.getFailedAttempts() < failedAttemptThreshold)) {
            activateJobsUnchecked(state, request);
          } else {
            completeOrEnqueueRequest(state, request);
          }
        });
  }

  private InFlightLongPollingActivateJobsRequestsState getJobTypeState(String jobType) {
    synchronized (jobTypeState) {
      return jobTypeState.computeIfAbsent(
          jobType, type -> new InFlightLongPollingActivateJobsRequestsState(type, metrics));
    }
  }

  private void activateJobsUnchecked(
      final InFlightLongPollingActivateJobsRequestsState state,
      final LongPollingActivateJobsRequest request) {

    final BrokerClusterState topology = brokerClient.getTopologyManager().getTopology();
    if (topology != null) {

      state.setActiveRequest(request);

      final int partitionsCount = topology.getPartitionsCount();
      activateJobsHandler.activateJobs(
          partitionsCount,
          request.getRequest(),
          request.getMaxJobsToActivate(),
          request.getType(),
          response -> onResponse(request, response),
          (remainingAmount, containedResourceExhaustedResponse) ->
              onCompleted(state, request, remainingAmount, containedResourceExhaustedResponse));
    }
  }

  private void onNotification(final String jobType) {
    LOG.trace("Received jobs available notification for type {}.", jobType);
    actor.run(() -> handlePendingRequests(jobType));
  }

  private void onCompleted(
      final InFlightLongPollingActivateJobsRequestsState state,
      final LongPollingActivateJobsRequest request,
      final Integer remainingAmount,
      final Boolean containedResourceExhaustedResponse) {
    state.removeActiveRequest();
    if (remainingAmount == request.getMaxJobsToActivate()) {
      if (containedResourceExhaustedResponse) {
        actor.submit(
            () ->
                request
                    .getResponseObserver()
                    .onError(
                        new BrokerErrorException(
                            new BrokerError(
                                ErrorCode.RESOURCE_EXHAUSTED,
                                "Some brokers returned resource exhausted"))));
      } else {
        actor.submit(
            () -> {
              state.incrementFailedAttempts(currentTimeMillis());
              activateJobs(request);
            });
      }
    } else {
      actor.submit(request::complete);
      actor.submit(() -> handlePendingRequests(request.getType()));
    }
  }

  private void onResponse(
      final LongPollingActivateJobsRequest request,
      final ActivateJobsResponse activateJobsResponse) {
    actor.submit(() -> request.onResponse(activateJobsResponse));
  }

  private void handlePendingRequests(final String jobType) {
    final InFlightLongPollingActivateJobsRequestsState state = getJobTypeState(jobType);

    state.resetFailedAttempts(0);

    if (state.getActiveRequest() == null) {

      final LongPollingActivateJobsRequest nextPendingRequest = state.getNextPendingRequest();

      if (nextPendingRequest != null) {
        LOG.trace("Unblocking ActivateJobsRequest {}", nextPendingRequest.getRequest());
        activateJobs(nextPendingRequest);
      } else {
        synchronized (jobTypeState) {
          jobTypeState.remove(jobType);
        }
      }
    }
  }

  private void completeOrEnqueueRequest(
      final InFlightLongPollingActivateJobsRequestsState state,
      final LongPollingActivateJobsRequest request) {
    if (request.isLongPollingDisabled()) {
      request.complete();
      return;
    }
    if (!request.isTimedOut()) {
      LOG.trace(
          "Worker '{}' asked for '{}' jobs of type '{}', but none are available. This request will"
              + " be kept open until a new job of this type is created or until timeout of '{}'.",
          request.getWorker(),
          request.getMaxJobsToActivate(),
          request.getType(),
          request.getLongPollingTimeout(longPollingTimeout));
      state.enqueueRequest(request);
      if (!request.hasScheduledTimer()) {
        addTimeOut(state, request);
      }
    }
  }

  private void addTimeOut(
      final InFlightLongPollingActivateJobsRequestsState state,
      final LongPollingActivateJobsRequest request) {
    ActorClock.currentTimeMillis();
    final Duration requestTimeout = request.getLongPollingTimeout(longPollingTimeout);
    final ScheduledTimer timeout =
        actor.runDelayed(
            requestTimeout,
            () -> {
              LOG.debug(
                  "Remove blocking request {} for job type {} after timeout of {}",
                  request.getRequest(),
                  request.getType(),
                  requestTimeout);
              state.removeRequest(request);
              request.timeout();
            });
    request.setScheduledTimer(timeout);
  }

  private void probe() {
    final long now = currentTimeMillis();
    jobTypeState.forEach(
        (type, state) -> {
          if (state.getActiveRequest() == null
              && state.getLastUpdatedTime() < (now - probeTimeoutMillis)) {
            final LongPollingActivateJobsRequest probeRequest = state.getNextPendingRequest();
            if (probeRequest != null) {
              activateJobsUnchecked(state, probeRequest);
            } else {
              // there are no blocked requests, so use next request as probe
              if (state.getFailedAttempts() >= failedAttemptThreshold) {
                state.resetFailedAttempts(failedAttemptThreshold - 1);
              }
            }
          }
        });
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private static final long DEFAULT_LONG_POLLING_TIMEOUT = 10_000; // 10 seconds
    private static final long DEFAULT_PROBE_TIMEOUT = 10_000; // 10 seconds
    // Minimum number of responses with jobCount 0 to infer that no jobs are available
    private static final int EMPTY_RESPONSE_THRESHOLD = 3;

    private BrokerClient brokerClient;
    private long longPollingTimeout = DEFAULT_LONG_POLLING_TIMEOUT;
    private long probeTimeoutMillis = DEFAULT_PROBE_TIMEOUT;
    private int minEmptyResponses = EMPTY_RESPONSE_THRESHOLD;

    public Builder setBrokerClient(final BrokerClient brokerClient) {
      this.brokerClient = brokerClient;
      return this;
    }

    public Builder setLongPollingTimeout(final long longPollingTimeout) {
      this.longPollingTimeout = longPollingTimeout;
      return this;
    }

    public Builder setProbeTimeoutMillis(final long probeTimeoutMillis) {
      this.probeTimeoutMillis = probeTimeoutMillis;
      return this;
    }

    public Builder setMinEmptyResponses(final int minEmptyResponses) {
      this.minEmptyResponses = minEmptyResponses;
      return this;
    }

    public LongPollingActivateJobsHandler build() {
      Objects.requireNonNull(brokerClient, "brokerClient");
      return new LongPollingActivateJobsHandler(
          brokerClient, longPollingTimeout, probeTimeoutMillis, minEmptyResponses);
    }
  }
}
