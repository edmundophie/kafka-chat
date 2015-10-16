package com.edmundophie.chat;

import com.edmundophie.rpc.Request;
import com.edmundophie.rpc.Response;
import com.fasterxml.jackson.core.JsonProcessingException;
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

import java.io.IOException;
import java.util.*;

/**
 * Created by edmundophie on 10/16/15.
 */
public class ChatServer {
    private static Producer responseProducer;
    private final static String BROKER_LIST = "localhost:9092";
    private final static String PRODUCER_TYPE= "sync";
    private final static String SERIALIZER = "kafka.serializer.StringEncoder";
    private final static String REQUEST_REQUIRED_ACK = "1";
    private final static String RPC_REQUEST_TOPIC_NAME = "rpcRequestTopic";
    private final static String ZOOKEEPER_SERVER = "localhost:2181";
    private final static String RPC_RESPONSE_TOPIC_NAME = "rpcResponseTopic";
    private final static String SERVER_CONSUMER_GROUP = "server-consumer-group";
    private final static int MAX_GENERATED_RANDOM_ACCOUNT_INT = 99999;

    private static Map<String, User> userMap;
    private static Map<String, Channel> channelMap;

    private ConsumerConnector consumerConnector;
    private ConsumerIterator<byte[], byte[]> consumerIterator;
    private ObjectMapper mapper;

    public ChatServer() {
        initResponseProducer();
        initRequestConsumer();
        mapper = new ObjectMapper();
    }

    public static void main(String[] args) throws JsonProcessingException {
        System.out.println("- Starting server...");
        initConfiguration();
        ChatServer server = new ChatServer();
        server.start();
        server.shutdown();
        responseProducer.close();
    }

    public static void initConfiguration() {
        userMap =  new HashMap<String, User>();
        channelMap =  new HashMap<String, Channel>();
    }

    private void initResponseProducer() {
        Properties props = new Properties();
        props.put("metadata.broker.list", BROKER_LIST);
        props.put("request.required.acks", REQUEST_REQUIRED_ACK);
        props.put("producer.type", PRODUCER_TYPE);
        props.put("serializer.class", SERIALIZER);
        props.put("retry.backoff.ms", "500");

        ProducerConfig config = new ProducerConfig(props);
        responseProducer = new Producer(config);

    }

    private void initRequestConsumer() {
        Properties props = new Properties();
        props.put("zookeeper.connect", ZOOKEEPER_SERVER);
        props.put("group.id", SERVER_CONSUMER_GROUP);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "largest");

        ConsumerConfig config = new ConsumerConfig(props);
        consumerConnector = Consumer.createJavaConsumerConnector(config);

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(RPC_REQUEST_TOPIC_NAME, 1);

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = consumerMap.get(RPC_REQUEST_TOPIC_NAME).get(0);

        consumerIterator = stream.iterator();
    }

    private static void sendRpcResponse(String message, String corrId) {
        KeyedMessage<String, String> msg = new KeyedMessage<String, String>(RPC_RESPONSE_TOPIC_NAME, corrId, message);
        responseProducer.send(msg);
    }

    private static void sendMessageToTopic(String message, String topic) {
        KeyedMessage<String, String> msg = new KeyedMessage<String, String>(topic, message);
        responseProducer.send(msg);
    }

    private void start() throws JsonProcessingException {
        System.out.println("- Server started");

        String requestMessage;
        String corrId;

        while (true) {
            if(consumerIterator.hasNext()) {
                MessageAndMetadata<byte[], byte[]> request = consumerIterator.next();
                corrId = new String(request.key());

                requestMessage = new String(request.message());
                String responseMessage = processRequest(requestMessage);
                sendRpcResponse(responseMessage, corrId);
            }
        }
    }

    private void shutdown() {
        consumerConnector.shutdown();
    }

    private String processRequest(String requestMessage) throws JsonProcessingException {
        Request request = null;
        try {
            request = mapper.readValue(requestMessage, Request.class);
        } catch (IOException e) {
            Response response = new Response();
            response.putStatus(false);
            response.setMessage("* Server Encountered An Error On Processing Message!");
            return mapper.writeValueAsString(response);
        }

        if (request.getCommand().equalsIgnoreCase("NICK")) {
            return login(request.getNickname());
        } else if (request.getCommand().equalsIgnoreCase("JOIN")) {
            return join(request.getNickname(), request.getChannelName());
        } else if (request.getCommand().equalsIgnoreCase("LEAVE")) {
            return leave(request.getNickname(), request.getChannelName());
        } else if (request.getCommand().equalsIgnoreCase("LOGOUT")) {
            return logout(request.getNickname());
        } else if (request.getCommand().equalsIgnoreCase("EXIT")) {
            return exit(request.getNickname());
        } else if (request.getCommand().equalsIgnoreCase("SEND")) {
            return sendMessage(request.getNickname(), request.getChannelName(), request.getMessage());
        } else if (request.getCommand().equalsIgnoreCase("BROADCAST")) {
            return broadcastMessage(request.getNickname(), request.getMessage());
        } else {
            Response response = new Response();
            response.putStatus(false);
            response.setMessage("* Unknown Message Command!");
            return mapper.writeValueAsString(response);
        }
    }

    private String login(String nickname) {
        System.out.println("- Login method invoked");
        StringBuilder message = new StringBuilder();

        if(nickname==null || nickname.isEmpty() || userMap.containsKey(nickname)) {
            if(userMap.containsKey(nickname)) message.append("* Username exist!\n");
            nickname = generateRandomNickname();
            message.append("* Random user generated\n");
        }
        message.append("* Successfully logged in as " + nickname);

        userMap.put(nickname, new User(nickname));

        Response response = new Response(true, message.toString(), nickname);
        return response.toString();
    }

    private static String generateRandomNickname() {
        String newNickname;
        Random random = new Random();
        do {
            newNickname = "user" + random.nextInt(MAX_GENERATED_RANDOM_ACCOUNT_INT);
        } while(userMap.containsKey(newNickname));

        return newNickname;
    }

    public static String join(String nickname, String channelName) {
        System.out.println("- " + nickname + " requested to join #" + channelName);

        List userChannelList = userMap.get(nickname).getJoinedChannel();
        StringBuilder message = new StringBuilder();
        Response response = new Response();

        if(userChannelList.contains(channelName)) {
            message.append("* You are already a member of #" + channelName);
            response.putStatus(false);
        } else {
            if(!channelMap.containsKey(channelName)) {
                channelMap.put(channelName, new Channel(channelName));
                message.append("* Created new channel #" + channelName + "\n");
            }

            userChannelList.add(channelName);
            message.append("* #" + channelName + " joined successfully");
            response.putStatus(true);
        }

        response.setMessage(message.toString());
        return response.toString();
    }

    public static String leave(String nickname, String channelName) {
        System.out.println("- " + nickname + " request to leave #" + channelName);

        StringBuilder message = new StringBuilder();
        Response response = new Response();

        if(!userMap.get(nickname).getJoinedChannel().contains(channelName)) {
            System.err.println("- Failed to leave channel. " + nickname + " is not a member of #" + channelName);
            message.append("* Failed to leave.\n* You are not a member of #" + channelName);
            response.putStatus(false);
        } else {
            userMap.get(nickname).getJoinedChannel().remove(channelName);
            response.putStatus(true);
            message.append("* You are no longer a member of #" + channelName);
        }

        response.setMessage(message.toString());
        return response.toString();
    }


    public static String logout(String nickname) {
        System.out.println("- " + nickname + " requested to logout");
        userMap.remove(nickname);

        Response response = new Response();
        response.putStatus(true);
        response.setMessage("* " + nickname + " have been logged out");

        return response.toString();
    }

    public static String exit(String nickname) {
        return logout(nickname);
    }


    public static String sendMessage(String nickname, String channelName, String message) {
        System.out.println("- " + nickname + " sends a message to #" + channelName);
        StringBuilder returnedMessage = new StringBuilder();
        Response response = new Response();

        List<String> userChannelList = userMap.get(nickname).getJoinedChannel();
        if(!userChannelList.contains(channelName)) {
            System.err.println("- Failed to send " + nickname + " message to #" + channelName + ". User is not a member of the channel.");
            returnedMessage.append("* You are not a member of #" + channelName);
            response.putStatus(false);
        } else {
            try {
                Message msg = new Message(nickname, message);
                distributeMessage(msg, channelName);
            } catch (IOException e) {
                e.printStackTrace();
                response.putStatus(false);
                response.setMessage("* Server Encountered An Error On Publishing the Message");
                return response.toString();
            }
            response.putStatus(true);
        }

        response.setMessage(returnedMessage.toString());
        return response.toString();
    }

    public static String broadcastMessage(String nickname, String message) {
        System.out.println("- " + nickname + " broadcasts a message");
        StringBuilder returnedMessage = new StringBuilder();
        Response response = new Response();

        List<String> userChannelList = userMap.get(nickname).getJoinedChannel();
        if(userChannelList.size()==0) {
            System.err.println("- Failed to send " + nickname + " message. No channel found.");
            returnedMessage.append("* Failed to send the message\n* You haven't join any channel yet");
            response.putStatus(false);
        } else {
            try {
                Message msg = new Message(nickname, message);
                distributeMessage(msg, userChannelList);
            } catch (IOException e) {
                e.printStackTrace();
                response.putStatus(false);
                response.setMessage("* Server Encountered An Error On Publishing the Message");
                return response.toString();
            }
            response.putStatus(true);
        }

        response.setMessage(returnedMessage.toString());
        return response.toString();
    }

    public static void distributeMessage(Message message, List<String> userChannelList) throws IOException {
        for(String channelName:userChannelList) {
            distributeMessage(message, channelName);
        }
    }

    public static void distributeMessage(Message message, String channelName) throws IOException {
        String enrichedMessage = "@" + channelName + " " + message.getSender()+ ": " + message.getText();
        sendMessageToTopic(enrichedMessage, channelName);
    }
}
