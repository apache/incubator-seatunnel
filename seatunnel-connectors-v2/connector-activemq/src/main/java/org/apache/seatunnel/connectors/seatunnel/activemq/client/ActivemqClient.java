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

package org.apache.seatunnel.connectors.seatunnel.activemq.client;

import org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqConfig;
import org.apache.seatunnel.connectors.seatunnel.activemq.exception.ActivemqConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.activemq.exception.ActivemqConnectorException;

import org.apache.activemq.ActiveMQConnectionFactory;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import java.nio.charset.StandardCharsets;

@Slf4j
@AllArgsConstructor
public class ActivemqClient {
    private final ActivemqConfig config;
    private final ActiveMQConnectionFactory connectionFactory;
    private final Connection connection;

    public ActivemqClient(ActivemqConfig config) {
        this.config = config;
        try {
            this.connectionFactory = getConnectionFactory();
            log.info("connection factory created");
            this.connection = createConnection(config);
            log.info("connection created");

        } catch (Exception e) {
            e.printStackTrace();
            throw new ActivemqConnectorException(
                    ActivemqConnectorErrorCode.CREATE_ACTIVEMQ_CLIENT_FAILED,
                    "Error while create AMQ client ");
        }
    }

    public ActiveMQConnectionFactory getConnectionFactory() {
        log.info("broker url : " + config.getUri());
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(config.getUri());

        if (config.getAlwaysSessionAsync() != null) {
            factory.setAlwaysSessionAsync(config.getAlwaysSessionAsync());
        }

        if (config.getClientID() != null) {
            factory.setClientID(config.getClientID());
        }

        if (config.getAlwaysSyncSend() != null) {
            factory.setAlwaysSyncSend(config.getAlwaysSyncSend());
        }

        if (config.getCheckForDuplicate() != null) {
            factory.setCheckForDuplicates(config.getCheckForDuplicate());
        }

        if (config.getCloseTimeout() != null) {
            factory.setCloseTimeout(config.getCloseTimeout());
        }

        if (config.getConsumerExpiryCheckEnabled() != null) {
            factory.setConsumerExpiryCheckEnabled(config.getConsumerExpiryCheckEnabled());
        }
        if (config.getDispatchAsync() != null) {
            factory.setDispatchAsync(config.getDispatchAsync());
        }

        if (config.getWarnAboutUnstartedConnectionTimeout() != null) {
            factory.setWarnAboutUnstartedConnectionTimeout(
                    config.getWarnAboutUnstartedConnectionTimeout());
        }

        if (config.getNestedMapAndListEnabled() != null) {
            factory.setNestedMapAndListEnabled(config.getNestedMapAndListEnabled());
        }

        return factory;
    }

    public void write(byte[] msg) {
        try {
            Session session = this.getSession();
            Destination destination = session.createQueue(config.getQueueName());
            MessageProducer producer = session.createProducer(destination);
            String messageBody = new String(msg, StandardCharsets.UTF_8);
            TextMessage objectMessage = session.createTextMessage(messageBody);
            producer.send(objectMessage);

        } catch (JMSException e) {
            throw new ActivemqConnectorException(
                    ActivemqConnectorErrorCode.SEND_MESSAGE_FAILED,
                    String.format(
                            "Cannot send AMQ message %s at %s",
                            config.getQueueName(), config.getClientID()),
                    e);
        }
    }

    public Session getSession() {
        try {
            this.connection.start();
            return this.connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        } catch (JMSException e) {
            throw new ActivemqConnectorException(
                    ActivemqConnectorErrorCode.CLOSE_SESSION_FAILED, e);
        }
    }

    public MessageConsumer getConsumer() {
        try {
            return this.getSession()
                    .createConsumer(this.getSession().createQueue(config.getQueueName()));
        } catch (JMSException e) {
            throw new ActivemqConnectorException(
                    ActivemqConnectorErrorCode.INITIALIZE_CONSUME_FAILED, e);
        }
    }

    public void close() {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (JMSException e) {
            throw new ActivemqConnectorException(
                    ActivemqConnectorErrorCode.CLOSE_CONNECTION_FAILED,
                    String.format(
                            "Error while closing AMQ connection with  %s", config.getQueueName()));
        }
    }

    private Connection createConnection(ActivemqConfig config) throws JMSException {
        if (config.getUsername() != null && config.getPassword() != null) {
            return connectionFactory.createConnection(config.getUsername(), config.getPassword());
        }
        return connectionFactory.createConnection();
    }
}
