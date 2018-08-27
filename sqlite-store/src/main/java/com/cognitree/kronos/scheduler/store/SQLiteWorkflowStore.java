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

package com.cognitree.kronos.scheduler.store;

import com.cognitree.kronos.model.definitions.Workflow;
import com.cognitree.kronos.model.definitions.Workflow.WorkflowTask;
import com.cognitree.kronos.model.definitions.WorkflowId;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A SQLite implementation of {@link WorkflowStore}.
 */
public class SQLiteWorkflowStore implements WorkflowStore {
    private static final Logger logger = LoggerFactory.getLogger(SQLiteWorkflowStore.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String INSERT_WORKFLOW = "INSERT INTO workflows VALUES (?,?,?,?)";
    private static final String LOAD_ALL_WORKFLOW_BY_NAMESPACE = "SELECT * FROM workflows " +
            "WHERE namespace = ?";
    private static final String UPDATE_WORKFLOW = "UPDATE workflows set description = ?, " +
            " tasks = ? where name = ? AND namespace = ?";
    private static final String DELETE_WORKFLOW = "DELETE FROM workflows where name = ? " +
            "AND namespace = ?";
    private static final String LOAD_WORKFLOW = "SELECT * FROM workflows where name = ? AND namespace = ?";
    private static final String DDL_CREATE_WORKFLOW_SQL = "CREATE TABLE IF NOT EXISTS workflows (" +
            "name string," +
            "namespace string," +
            "description string," +
            "tasks string," +
            "PRIMARY KEY(name, namespace)" +
            ")";
    private static final TypeReference<List<WorkflowTask>> WORKFLOW_TASK_LIST_TYPE_REF =
            new TypeReference<List<WorkflowTask>>() {
            };

    private BasicDataSource dataSource;

    @Override
    public void init(ObjectNode storeConfig) throws Exception {
        logger.info("Initializing SQLite workflow store");
        initDataSource(storeConfig);
        initWorkflowStore();
    }

    private void initDataSource(ObjectNode storeConfig) {
        dataSource = new BasicDataSource();
        dataSource.setUrl(storeConfig.get("connectionURL").asText());
        if (storeConfig.hasNonNull("username")) {
            dataSource.setUsername(storeConfig.get("username").asText());
            if (storeConfig.hasNonNull("password")) {
                dataSource.setPassword(storeConfig.get("password").asText());
            }
        }
        if (storeConfig.hasNonNull("minIdleConnection")) {
            dataSource.setMinIdle(storeConfig.get("minIdleConnection").asInt());
        }
        if (storeConfig.hasNonNull("maxIdleConnection")) {
            dataSource.setMaxIdle(storeConfig.get("maxIdleConnection").asInt());
        }
        if (storeConfig.hasNonNull("maxOpenPreparedStatements")) {
            dataSource.setMaxOpenPreparedStatements(storeConfig.get("maxOpenPreparedStatements").asInt());
        }
    }

    private void initWorkflowStore() throws SQLException {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.setQueryTimeout(30);
            statement.executeUpdate(DDL_CREATE_WORKFLOW_SQL);
        }
    }

    @Override
    public void store(Workflow workflow) {
        logger.debug("Received request to store workflow {}", workflow);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(INSERT_WORKFLOW)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, workflow.getName());
            preparedStatement.setString(++paramIndex, workflow.getNamespace());
            preparedStatement.setString(++paramIndex, workflow.getDescription());
            preparedStatement.setString(++paramIndex, MAPPER.writeValueAsString(workflow.getTasks()));
            preparedStatement.execute();
        } catch (Exception e) {
            logger.error("Error storing workflow {} into database", workflow, e);
        }
    }

    @Override
    public List<Workflow> load(String namespace) {
        logger.debug("Received request to get all workflows in namespace {}", namespace);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(LOAD_ALL_WORKFLOW_BY_NAMESPACE)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, namespace);
            final ResultSet resultSet = preparedStatement.executeQuery();
            List<Workflow> workflows = new ArrayList<>();
            while (resultSet.next()) {
                workflows.add(getWorkflowDefinition(resultSet));
            }
            return workflows;
        } catch (Exception e) {
            logger.error("Error fetching all workflows from database in namespace {}", namespace, e);
            return Collections.emptyList();
        }
    }

    @Override
    public Workflow load(WorkflowId workflowId) {
        logger.debug("Received request to load workflow with id {}", workflowId);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(LOAD_WORKFLOW)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, workflowId.getName());
            preparedStatement.setString(++paramIndex, workflowId.getNamespace());
            final ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return getWorkflowDefinition(resultSet);
            }
        } catch (Exception e) {
            logger.error("Error fetching workflow with id {} from database", workflowId, e);
        }
        return null;
    }

    @Override
    public void update(Workflow workflow) {
        logger.debug("Received request to update workflow {}", workflow);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(UPDATE_WORKFLOW)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, workflow.getDescription());
            preparedStatement.setString(++paramIndex, MAPPER.writeValueAsString(workflow.getTasks()));
            preparedStatement.setString(++paramIndex, workflow.getName());
            preparedStatement.setString(++paramIndex, workflow.getNamespace());
            preparedStatement.execute();
        } catch (Exception e) {
            logger.error("Error updating workflow {} into database", workflow, e);
        }
    }

    @Override
    public void delete(WorkflowId workflowId) {
        logger.debug("Received request to delete workflow with id {}", workflowId);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(DELETE_WORKFLOW)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, workflowId.getName());
            preparedStatement.setString(++paramIndex, workflowId.getNamespace());
            preparedStatement.executeUpdate();
        } catch (Exception e) {
            logger.error("Error deleting workflow with id {} from database", workflowId, e);
        }
    }

    private Workflow getWorkflowDefinition(ResultSet resultSet) throws Exception {
        int paramIndex = 0;
        Workflow workflow = new Workflow();
        workflow.setName(resultSet.getString(++paramIndex));
        workflow.setNamespace(resultSet.getString(++paramIndex));
        workflow.setDescription(resultSet.getString(++paramIndex));
        workflow.setTasks(MAPPER.readValue(resultSet.getString(++paramIndex), WORKFLOW_TASK_LIST_TYPE_REF));
        return workflow;
    }

    @Override
    public void stop() {
        try {
            dataSource.close();
        } catch (SQLException e) {
            logger.error("Error closing data source", e);
        }
    }
}
