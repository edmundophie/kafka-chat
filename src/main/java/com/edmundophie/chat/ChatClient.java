package com.edmundophie.chat;

import com.edmundophie.rpc.Request;
import com.edmundophie.rpc.Response;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.message.MessageAndMetadata;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import scala.collection.immutable.Stream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

/**
 * Created by edmundophie on 10/15/15.
 */
public class ChatClient {
    private static Producer requestProducer;
    private final static String BROKER_LIST = "localhost:9092";
    private final static String PRODUCER_TYPE= "sync";
    private final static String SERIALIZER = "kafka.serializer.StringEncoder";
    private final static String REQUEST_REQUIRED_ACK = "1";
    private final static String RPC_REQUEST_TOPIC_NAME = "rpcRequestTopic";
    private final static String ZOOKEEPER_SERVER = "localhost:2181";
    private final static String RPC_RESPONSE_TOPIC_NAME = "rpcResponseTopic";
    private static ConsumerConfig consumerConfig;

    private ConsumerConnector consumerConnector;
    private ConsumerIterator<byte[], byte[]> consumerIterator;
    private String consumerGroup;

    private ObjectMapper mapper;
    private boolean isLoggedIn;
    private String nickname;
    private Map<String, Boolean> topicListenerStatusMap;
    private Map<String, MessageConsumerImpl> topicListenerThreadMap;
    private Map<String, ConsumerConnector> topicListenerConsumerConnectorMap;

    public ChatClient() {
        isLoggedIn = false;
        nickname = "";
        mapper = new ObjectMapper();
        consumerGroup = UUID.randomUUID().toString();
        initResponseConsumer();
        topicListenerStatusMap = new HashMap<String, Boolean>();
        topicListenerThreadMap = new HashMap<String, MessageConsumerImpl>();
        topicListenerConsumerConnectorMap = new HashMap<String, ConsumerConnector>();
    }

    public static void main(String[] args) {
        System.out.println("Starting client....");

        initRequestProducer();
        ChatClient client = new ChatClient();
        try {
            client.start();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            client.shutdown();
            requestProducer.close();
        }
    }

    private static void initRequestProducer() {
        Properties props = new Properties();
        props.put("metadata.broker.list", BROKER_LIST);
        props.put("request.required.acks", REQUEST_REQUIRED_ACK);
        props.put("producer.type", PRODUCER_TYPE);
        props.put("serializer.class", SERIALIZER);

        ProducerConfig config = new ProducerConfig(props);
        requestProducer = new Producer(config);
    }

    private static void printInvalidCommand() {
        System.err.println("* Invalid Command");
    }

    private void initResponseConsumer() {
        createConsumerConfig();
        consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(RPC_RESPONSE_TOPIC_NAME, 1);

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = consumerMap.get(RPC_RESPONSE_TOPIC_NAME).get(0);

        consumerIterator = stream.iterator();
    }

    private void createConsumerConfig() {
        Properties consumerProperties = new Properties();
        consumerProperties.put("zookeeper.connect", ZOOKEEPER_SERVER);
        consumerProperties.put("group.id", UUID.randomUUID().toString());
        consumerProperties.put("zookeeper.session.timeout.ms", "400");
        consumerProperties.put("zookeeper.sync.time.ms", "200");
        consumerProperties.put("auto.commit.interval.ms", "1000");
        consumerProperties.put("auto.offset.reset", "largest");

        consumerConfig = new ConsumerConfig(consumerProperties);
    }

    private String sendRpcRequest(String message) {
        String corrId = UUID.randomUUID().toString();
        KeyedMessage<String, String> msg = new KeyedMessage<String, String>(RPC_REQUEST_TOPIC_NAME, corrId, message);
        requestProducer.send(msg);

        return getRpcResponse(corrId);
    }

    private String getRpcResponse(String corrId) {
        String responseMessage = null;
        while (true) {
            if(consumerIterator.hasNext()) {
                MessageAndMetadata<byte[], byte[]> response = consumerIterator.next();
                if(new String(response.key()).equals(corrId)) {
                    responseMessage = new String(response.message());
                    break;
                }
            }
        }

        return responseMessage;
    }

    private void shutdown() {
        consumerConnector.shutdown();
    }

    private void start() throws Exception {
        System.out.println("* Client started");

        String command = null;
        do {
            String input = new BufferedReader(new InputStreamReader(System.in)).readLine().trim();

            if(input.isEmpty())
                printInvalidCommand();
            else {
                String parameter = "";
                int i = input.indexOf(" ");

                if (i > -1) {
                    command = input.substring(0, i);
                    parameter = input.substring(i + 1);
                } else
                    command = input;

                if (command.equalsIgnoreCase("NICK")) {
                    login(command.toUpperCase(), parameter);
                } else if (command.equalsIgnoreCase("JOIN")) {
                    join(command.toUpperCase(), parameter);
                } else if (command.equalsIgnoreCase("LEAVE")) {
                    leave(command.toUpperCase(), parameter);
                } else if (command.equalsIgnoreCase("LOGOUT")) {
                    logout(command.toUpperCase());
                } else if (command.equalsIgnoreCase("EXIT")) {
                    exit(command.toUpperCase());
                } else if (command.charAt(0) == '@') {
                    sendMessage(command.substring(1), parameter);
                } else {
                    broadcastMessage(input);
                }
            }
        } while (!command.equalsIgnoreCase("EXIT"));
    }

    private void login(String command, String parameter) throws Exception {
        if(isLoggedIn) {
            System.err.println("* Please logout first!");
        } else {

            Request request = new Request();
            request.setCommand(command);
            request.setNickname(parameter);

            String responseJson = sendRpcRequest(request.toString());
            Response response = mapper.readValue(responseJson, Response.class);

            if(response.isStatus()) {
                nickname = response.getNickname();
                isLoggedIn = true;
                System.out.println(response.getMessage());
                createConsumerConfig();
            } else {
                System.err.println(response.getMessage());
            }
        }
    }

    private void join(String command, String parameter) throws Exception {
        if(!isLoggedIn) System.err.println("* Please login first!");
        else if(parameter==null || parameter.isEmpty()) printInvalidCommand();
        else {
            Request request = new Request();
            request.setCommand(command);
            request.setChannelName(parameter);
            request.setNickname(nickname);

            String responseJson = sendRpcRequest(request.toString());
            Response response = mapper.readValue(responseJson, Response.class);

            if(response.isStatus()) {
                addTopicListener(parameter);
                System.out.println(response.getMessage());
            } else
                System.err.println(response.getMessage());
        }
    }

    private void leave(String command, String parameter) throws Exception {
        if(!isLoggedIn) System.err.println("* Please login first!");
        else if(parameter==null || parameter.isEmpty()) printInvalidCommand();
        else  {
            Request request = new Request();
            request.setCommand(command);
            request.setChannelName(parameter);
            request.setNickname(nickname);

            String responseJson = sendRpcRequest(request.toString());
            Response response = mapper.readValue(responseJson, Response.class);

            if(response.isStatus()) {
                removeTopicListener(parameter);
                System.out.println(response.getMessage());
            } else
                System.err.println(response.getMessage());
        }
    }

    private void logout(String command) throws Exception {
        if(!isLoggedIn) System.err.println("* Please login first!");
        else {
            Request request = new Request();
            request.setCommand(command);
            request.setNickname(nickname);

            String responseJson = sendRpcRequest(request.toString());
            Response response = mapper.readValue(responseJson, Response.class);

            if(response.isStatus()) {
                isLoggedIn = false;
                nickname = "";
                removeAllTopicListener();
                System.out.println(response.getMessage());
            } else {
                System.err.println(response.getMessage());
            }
        }
    }

    private void exit(String command) throws Exception {
        if(isLoggedIn)
            logout(command);

        System.out.println("* Program exited");
    }

    private void sendMessage(String channelName, String message) throws Exception {
        if(!isLoggedIn) System.err.println("* Please login first!");
        else if(message==null || message.isEmpty()) printInvalidCommand();
        else {
            Request request = new Request();
            request.setCommand("SEND");
            request.setChannelName(channelName);
            request.setMessage(message);
            request.setNickname(nickname);

            String responseJson = sendRpcRequest(request.toString());
            Response response = mapper.readValue(responseJson, Response.class);

            if(!response.isStatus()) {
                System.err.println(response.getMessage());
            }
        }
    }

    private void broadcastMessage(String message) throws Exception {
        if(!isLoggedIn) System.err.println("Please login first!");
        else {
            Request request = new Request();
            request.setCommand("BROADCAST");
            request.setMessage(message);
            request.setNickname(nickname);

            String responseJson = sendRpcRequest(request.toString());
            Response response = mapper.readValue(responseJson, Response.class);

            if(!response.isStatus()) {
                System.err.println(response.getMessage());
            }
        }
    }

    private void addTopicListener(String topic) {
        ConsumerConnector topicConsumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
        topicListenerConsumerConnectorMap.put(topic, topicConsumerConnector);

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, 1);

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = topicConsumerConnector.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);

        MessageConsumerImpl topicListenerThread = new MessageConsumerImpl(stream);
        topicListenerThread.start();
        topicListenerThreadMap.put(topic, topicListenerThread);
    }

    private void removeTopicListener(String topic) throws InterruptedException {
        topicListenerThreadMap.get(topic).terminate();
        topicListenerConsumerConnectorMap.get(topic).shutdown();

        topicListenerThreadMap.remove(topic);
        topicListenerConsumerConnectorMap.remove(topic);
    }

    private void removeAllTopicListener() throws InterruptedException {
        for(String topic:topicListenerThreadMap.keySet()) {
            topicListenerThreadMap.get(topic).terminate();
            topicListenerConsumerConnectorMap.get(topic).shutdown();
        }

        topicListenerThreadMap.clear();
        topicListenerConsumerConnectorMap.clear();
    }
}
