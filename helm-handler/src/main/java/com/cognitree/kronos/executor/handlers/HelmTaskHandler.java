/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cognitree.kronos.executor.handlers;

import com.cognitree.kronos.executor.model.TaskResult;
import com.cognitree.kronos.model.Task;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import hapi.chart.ChartOuterClass.Chart;
import hapi.release.ReleaseOuterClass.Release;
import hapi.release.StatusOuterClass.Status.Code;
import hapi.services.tiller.Tiller.GetReleaseStatusRequest;
import hapi.services.tiller.Tiller.GetReleaseStatusResponse;
import hapi.services.tiller.Tiller.InstallReleaseRequest;
import hapi.services.tiller.Tiller.InstallReleaseRequest.Builder;
import hapi.services.tiller.Tiller.UninstallReleaseRequest;
import io.fabric8.kubernetes.api.model.batch.Job;
import io.fabric8.kubernetes.api.model.batch.JobList;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.kamranzafar.jtar.TarInputStream;
import org.microbean.helm.ReleaseManager;
import org.microbean.helm.Tiller;
import org.microbean.helm.chart.DirectoryChartLoader;
import org.microbean.helm.chart.TapeArchiveChartLoader;
import org.microbean.helm.chart.URLChartLoader;
import org.microbean.helm.chart.ZipInputStreamChartLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.zip.ZipInputStream;

import static com.cognitree.kronos.executor.handlers.ChartType.directory;
import static hapi.services.tiller.Tiller.ListReleasesRequest;
import static hapi.services.tiller.Tiller.ListReleasesResponse;
import static hapi.services.tiller.Tiller.UpdateReleaseRequest;

/**
 * A {@link TaskHandler} implementation to submit helm charts to k8s cluster with given values.
 */
public class HelmTaskHandler implements TaskHandler {
    private static final Logger logger = LoggerFactory.getLogger(HelmTaskHandler.class);

    private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REF = new TypeReference<Map<String, Object>>() {
    };

    private static final String PROP_RELEASE_NAME = "releaseName";
    private static final String PROP_RELEASE_PREFIX = "releaseNamePrefix";
    private static final String PROP_CHART_TYPE = "chartType";
    private static final String PROP_CHART_PATH = "chartPath";
    private static final String PROP_NAMESPACE = "namespace";
    private static final String PROP_VALUES = "values";
    private static final String PROP_VALUES_FILE = "valuesFile";
    private static final String PROP_TIMEOUT = "timeout";
    private static final String PROP_IGNORE_JOB_STATUS = "ignoreJobStatus";

    private static final ChartType DEFAULT_CHART_TYPE = directory;
    private static final String DEFAULT_RELEASE_PREFIX = "release";
    private static final long DEFAULT_HELM_TIMEOUT = 300L;
    private static final boolean DEFAULT_IGNORE_JOB_STATUS = false;
    private static final int MAX_RETRY_COUNT = 3;
    private static final int RETRY_SLEEP_INTERVAL = 5000;

    private Task task;
    private boolean abort = false;
    private Release release;

    @Override
    public void init(Task task, ObjectNode config) {
        this.task = task;
    }

    @Override
    public TaskResult execute() {
        logger.info("received request to execute task {}", task);
        final Map<String, Object> taskProperties = task.getProperties();

        if (!taskProperties.containsKey(PROP_CHART_PATH)) {
            return new TaskResult(false, "missing mandatory property: " + PROP_CHART_PATH);
        }
        String releaseName;
        if (isRetry(task)) {
            releaseName = (String) task.getContext().get(PROP_RELEASE_NAME);
        } else if (taskProperties.containsKey(PROP_RELEASE_NAME)) {
            if (taskProperties.containsKey(PROP_RELEASE_PREFIX)) {
                releaseName = getProperty(taskProperties, PROP_RELEASE_PREFIX)
                        + "-" + getProperty(taskProperties, PROP_RELEASE_NAME);
            } else {
                releaseName = getProperty(taskProperties, PROP_RELEASE_NAME);
            }
        } else {
            releaseName = taskProperties.getOrDefault(PROP_RELEASE_PREFIX, DEFAULT_RELEASE_PREFIX)
                    + "-" + System.currentTimeMillis();
        }
        return execute(task, releaseName, 0);
    }

    /**
     * Only in case of retry the previously resolved release name is available in the task context.
     *
     * @param task
     * @return
     */
    private boolean isRetry(Task task) {
        return task.getContext() != null && task.getContext().containsKey(PROP_RELEASE_NAME);
    }

    private TaskResult execute(Task task, String releaseName, int retryCount) {
        logger.info("Installing helm release: {}, retry count {}", releaseName, retryCount);
        final Map<String, Object> taskResult = new HashMap<>();
        taskResult.put(PROP_RELEASE_NAME, releaseName);

        final Map<String, Object> taskProperties = task.getProperties();

        try (final DefaultKubernetesClient kubernetesClient = new DefaultKubernetesClient();
             final Tiller tiller = new Tiller(kubernetesClient);
             final ReleaseManager releaseManager = new ReleaseManager(tiller)) {

            // check existing releases in helm with same name
            // If it exists upgrade the release if it failed or wait for completion if already deployed.
            // Helm release with same name will exists in case of retry
            Iterator<ListReleasesResponse> releases = releaseManager.list(ListReleasesRequest.newBuilder()
                    .setFilter(releaseName)
                    .build());
            release = null;
            while (releases.hasNext()) {
                final Release existingRelease = releases.next().getReleases(0); // get the first release
                if (existingRelease.getName().equals(releaseName)) {
                    release = existingRelease;
                    break;
                }
            }
            if (release != null) { // is a retry case
                Code releaseStatus = getHelmReleaseStatus(releaseManager, release.getName());
                switch (releaseStatus) {
                    case FAILED:
                        logger.info("Redeploying failed helm release: {}", releaseName);
                        release = updateRelease(releaseName, taskProperties, releaseManager);
                        logger.info("Successfully redeployed helm release: {} in namespace: {}",
                                release.getName(), release.getNamespace());
                        // Post the deployment, wait for the deployment to complete.
                    case DEPLOYED:
                        // do nothing
                        break;
                    default:
                        throw new HelmExecutionException("Error deploying helm chart, current state is: " + releaseStatus);
                }
            } else {
                release = installRelease(releaseName, taskProperties, releaseManager);
                logger.info("Successfully installed release: {} in namespace: {}", releaseName, release.getNamespace());
            }
            boolean ignoreJobStatus = (boolean) taskProperties.getOrDefault(PROP_IGNORE_JOB_STATUS, DEFAULT_IGNORE_JOB_STATUS);
            waitForReleaseAndJobCompletion(releaseManager, kubernetesClient, releaseName,
                    release.getNamespace(), ignoreJobStatus);
            logger.info("Successfully completed release: {} in namespace {}", releaseName,
                    release.getNamespace());
            return new TaskResult(true, null, taskResult);
        } catch (HelmExecutionException e) {
            logger.error("Error deploying helm chart with release name {}", releaseName, e);
            return new TaskResult(false, "error deploying helm chart. error : " + e.getMessage(), taskResult);
        } catch (Exception e) {
            if (retryCount >= MAX_RETRY_COUNT) {
                logger.error("Error deploying helm chart with release name {} after {} retries, failing the task",
                        releaseName, MAX_RETRY_COUNT, e);
                return new TaskResult(false, "error deploying helm chart. error : " + e.getMessage(), taskResult);
            } else {
                logger.error("Error deploying helm chart with release name {}, retrying", releaseName, e);
                try {
                    Thread.sleep(RETRY_SLEEP_INTERVAL);
                } catch (InterruptedException ignored) {
                }
                return execute(task, releaseName, ++retryCount);
            }
        }
    }

    private void waitForReleaseAndJobCompletion(ReleaseManager releaseManager, KubernetesClient kubernetesClient,
                                                String releaseName, String namespace, boolean ignoreJobStatus)
            throws IOException, HelmExecutionException, ExecutionException, InterruptedException {
        logger.info("Waiting for release {} in namespace {} to be deployed with ignoreJobStatus {}.",
                releaseName, namespace, ignoreJobStatus);
        waitForReleaseToDeploy(releaseManager, releaseName, namespace);
        if (!ignoreJobStatus) {
            waitForJobCompletion(kubernetesClient, releaseName, namespace);
        }
    }

    private void waitForReleaseToDeploy(ReleaseManager releaseManager, String releaseName, String namespace)
            throws IOException, HelmExecutionException, ExecutionException, InterruptedException {
        boolean deployed = false;
        while (!deployed) {
            Code statusCode = getHelmReleaseStatus(releaseManager, releaseName);
            switch (statusCode) {
                case DEPLOYED:
                    logger.info("Successfully deployed release {} in namespace {}", releaseName, namespace);
                    deployed = true;
                    break;
                case PENDING_INSTALL:
                case PENDING_UPGRADE:
                case PENDING_ROLLBACK:
                    logger.info("waiting for release {} to complete under namespace {} , current state is: {}",
                            releaseName, namespace, statusCode);
                    break;
                case UNKNOWN:
                case SUPERSEDED:
                case DELETING:
                case UNRECOGNIZED:
                case DELETED:
                case FAILED:
                default:
                    logger.error("failed to deploy release {} in namespace {}, current state is: {}",
                            releaseName, namespace, statusCode);
                    throw new HelmExecutionException("failed to deploy " +
                            " deploy helm chart with release name " + releaseName +
                            " in namespace " + namespace + " current state is: " + statusCode);
            }
        }
    }

    private Code getHelmReleaseStatus(ReleaseManager releaseManager, String releaseName)
            throws IOException, ExecutionException, InterruptedException {
        final GetReleaseStatusRequest releaseStatusRequest = GetReleaseStatusRequest.newBuilder()
                .setName(releaseName)
                .build();
        GetReleaseStatusResponse releaseStatusResponse = releaseManager.getStatus(releaseStatusRequest).get();
        return releaseStatusResponse.getInfo().getStatus().getCode();
    }

    private void waitForJobCompletion(KubernetesClient kubernetesClient, String releaseName, String namespace)
            throws HelmExecutionException {
        logger.info("waiting for job to complete, deployed as part of release {}, namespace {}",
                releaseName, namespace);
        while (true) {
            if (abort) {
                logger.warn("Task has been aborted");
                throw new HelmExecutionException("Task has been aborted");
            }
            boolean jobCompleted = true;
            JobList jobList = getJobs(kubernetesClient, releaseName, namespace, MAX_RETRY_COUNT);
            List<Job> items = jobList.getItems();
            for (Job item : items) {
                if (item.getStatus().getSucceeded() == null || item.getStatus().getSucceeded().equals(0)) {
                    logger.debug("Job [" + item.getMetadata().getName() + "] is still active");
                    jobCompleted = false;
                    try {
                        Thread.sleep(RETRY_SLEEP_INTERVAL);
                    } catch (InterruptedException ignored) {
                    }
                    break;
                }
                if (item.getStatus().getFailed() != null && item.getStatus().getFailed().equals(1)) {
                    throw new HelmExecutionException("Job [" + item.getMetadata().getName() + "] failed " +
                            " deployed as part of helm release " + releaseName +
                            " in namespace " + namespace);
                }
            }
            if (jobCompleted) {
                logger.info("all jobs completed execution deployed as part of helm release {}, namespace {}",
                        releaseName, namespace);
                return;
            }
        }
    }

    private JobList getJobs(KubernetesClient kubernetesClient, String releaseName, String namespace, int jobRetryCount) {
        try {
            // we see a timeout exception communicating with k8s api server, retry the request for 3 times before failing
            return kubernetesClient.batch().jobs().inNamespace(namespace).withLabel("release", releaseName).list();
        } catch (Exception e) {
            logger.warn("Error communicating with kubernetes API server while fetching jobs, " +
                    "retrying request with retry count {}", jobRetryCount);
            logger.debug("Error communicating with kubernetes API server while fetching jobs", e);
            if (jobRetryCount > 0) {
                return getJobs(kubernetesClient, releaseName, namespace, --jobRetryCount);
            }
            throw e;
        }
    }

    private Builder buildInstallRequest(String releaseName, Map<String, Object> taskProperties)
            throws HelmExecutionException {
        try {
            final Builder requestBuilder = InstallReleaseRequest.newBuilder()
                    .setTimeout(DEFAULT_HELM_TIMEOUT)
                    .setName(releaseName) // Set the Helm release name
                    .setWait(true); // Wait for Pods to be ready

            if (taskProperties.containsKey(PROP_TIMEOUT)) {
                requestBuilder.setTimeout(Long.parseLong(getProperty(taskProperties, PROP_TIMEOUT)));
            }

            if (taskProperties.containsKey(PROP_NAMESPACE)) {
                requestBuilder.setNamespace(getProperty(taskProperties, PROP_NAMESPACE));
            }
            final Map<String, Object> valuesMap = getValues(taskProperties);
            if (!valuesMap.isEmpty()) {
                requestBuilder.getValuesBuilder().setRaw(new Yaml().dump(valuesMap));
            }
            return requestBuilder;
        } catch (IOException e) {
            logger.info("Error building install request for release {} with properties {}", releaseName, taskProperties, e);
            throw new HelmExecutionException("error building install request: " + e.getMessage(), e.getCause());
        }
    }

    private UpdateReleaseRequest.Builder buildUpdateRequest(String releaseName, Map<String, Object> taskProperties) throws HelmExecutionException {
        try {
            final UpdateReleaseRequest.Builder requestBuilder = UpdateReleaseRequest.newBuilder()
                    .setTimeout(DEFAULT_HELM_TIMEOUT)
                    .setName(releaseName) // Set the Helm release name
                    .setWait(true); // Wait for Pods to be ready

            if (taskProperties.containsKey(PROP_TIMEOUT)) {
                requestBuilder.setTimeout(Long.parseLong(getProperty(taskProperties, PROP_TIMEOUT)));
            }
            final Map<String, Object> valuesMap = getValues(taskProperties);
            if (!valuesMap.isEmpty()) {
                requestBuilder.getValuesBuilder().setRaw(new Yaml().dump(valuesMap));
            }
            return requestBuilder;
        } catch (IOException e) {
            logger.info("Error building update request for release {} with properties {}", releaseName, taskProperties, e);
            throw new HelmExecutionException("error building install request: " + e.getMessage(), e.getCause());
        }
    }

    /**
     * values passed explicitly via PROP_VALUES has precedence over values defined in PROP_VALUES_FILE
     */
    private Map<String, Object> getValues(Map<String, Object> taskProperties) throws IOException {
        final Map<String, Object> valuesMap = new HashMap<>();
        if (taskProperties.containsKey(PROP_VALUES_FILE)) {
            String valuesFilePath = (String) taskProperties.get(PROP_VALUES_FILE);
            valuesMap.putAll(MAPPER.readValue(new File(valuesFilePath), MAP_TYPE_REF));
        }
        if (taskProperties.containsKey(PROP_VALUES)) {
            valuesMap.putAll(MAPPER.convertValue(taskProperties.get(PROP_VALUES), MAP_TYPE_REF));
        }
        return valuesMap;
    }

    private Chart.Builder loadHelmChart(Map<String, Object> taskProperties) throws IOException, HelmExecutionException {
        final ChartType chartType;
        if (taskProperties.containsKey(PROP_CHART_TYPE)) {
            chartType = ChartType.valueOf(getProperty(taskProperties, PROP_CHART_TYPE));
        } else {
            chartType = DEFAULT_CHART_TYPE;
        }
        final String chartPath = getProperty(taskProperties, PROP_CHART_PATH);
        Chart.Builder helmChart = null;
        switch (chartType) {
            case directory:
                File chartDir = new File(chartPath);
                if (!chartDir.exists()) {
                    throw new HelmExecutionException("invalid chart path: " + chartPath);
                }
                try (DirectoryChartLoader directoryChartLoader = new DirectoryChartLoader()) {
                    helmChart = directoryChartLoader.load(chartDir.toPath());
                }
                break;
            case zip:
                File zipFile = new File(chartPath);
                ZipInputStream zipInputStream = new ZipInputStream(new FileInputStream(zipFile));
                try (ZipInputStreamChartLoader zipInputStreamChartLoader = new ZipInputStreamChartLoader()) {
                    helmChart = zipInputStreamChartLoader.load(zipInputStream);
                }
                break;
            case tape:
                File tarFile = new File(chartPath);
                TarInputStream tarInputStream = new TarInputStream(new FileInputStream(tarFile));
                try (TapeArchiveChartLoader tapeArchiveChartLoader = new TapeArchiveChartLoader()) {
                    helmChart = tapeArchiveChartLoader.load(tarInputStream);
                }
                break;
            case url:
                final URI uri = URI.create(chartPath);
                final URL url = uri.toURL();
                try (final URLChartLoader chartLoader = new URLChartLoader()) {
                    helmChart = chartLoader.load(url);
                }
                break;
        }
        return helmChart;
    }

    private String getProperty(Map<String, Object> properties, String key) {
        return String.valueOf(properties.get(key));
    }

    private synchronized Release installRelease(String releaseName, Map<String, Object> taskProperties,
                                                ReleaseManager releaseManager)
            throws IOException, HelmExecutionException, InterruptedException, ExecutionException {
        if (abort) {
            logger.warn("Task has been aborted, do not install");
            throw new HelmExecutionException("Task has been aborted");
        }
        final Chart.Builder chart = loadHelmChart(taskProperties);
        final Builder requestBuilder = buildInstallRequest(releaseName, taskProperties);
        return releaseManager.install(requestBuilder, chart).get().getRelease();
    }

    private synchronized Release updateRelease(String releaseName, Map<String, Object> taskProperties,
                                               ReleaseManager releaseManager)
            throws IOException, HelmExecutionException, InterruptedException, ExecutionException {
        if (abort) {
            logger.warn("Task has been aborted, do not update");
            throw new HelmExecutionException("Task has been aborted");
        }
        final Chart.Builder chart = loadHelmChart(taskProperties);
        final UpdateReleaseRequest.Builder requestBuilder = buildUpdateRequest(releaseName, taskProperties);
        return releaseManager.update(requestBuilder, chart)
                .get().getRelease();
    }

    private synchronized Release deleteRelease(String releaseName, ReleaseManager releaseManager)
            throws IOException, ExecutionException, InterruptedException {
        final UninstallReleaseRequest uninstallReleaseRequest = UninstallReleaseRequest.newBuilder()
                .setName(releaseName).build();
        return releaseManager.uninstall(uninstallReleaseRequest).get().getRelease();
    }

    @Override
    public void abort() {
        logger.info("Received request to abort task {}", task);
        abort = true;
        if (release != null) {
            try (final DefaultKubernetesClient kubernetesClient = new DefaultKubernetesClient();
                 final Tiller tiller = new Tiller(kubernetesClient);
                 final ReleaseManager releaseManager = new ReleaseManager(tiller)) {
                deleteRelease(release.getName(), releaseManager);
            } catch (IOException | InterruptedException | ExecutionException e) {
                logger.error("Error uninstalling helm release {}", release.getName(), e);
            }
        }
    }
}