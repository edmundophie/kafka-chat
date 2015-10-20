# kafka-chat
A CLI Chat Program Based on Apache Kafka

## Requirements
 - JDK >= 1.7
 - [Maven](https://maven.apache.org/download.cgi) 
 - [Apache Kafka 0.8.2.2](http://www.http://kafka.apache.org/) (with `ZooKeeper` included)


## How to Build
1. Resolve maven dependency  

	 ```
	 $ mvn dependency:copy-dependencies
	 ```
2. Build `jar` using maven `mvn`  

	 ```
	 $ mvn package
	 ```

## How to Run	 
1. Start `ZooKeeper` server on your machine

	 ```
	 $ bin/zookeeper-server-start.sh config/zookeeper.properties
	 ```
2. Start `Kafka` server on your machine

	 ```
	 $ bin/kafka-server-start.sh config/server.properties
	 ```
2. Run `ChatServer` from the generated `jar` in `target` folder  

	 ```
	 $ java -cp target/dependency/*:target/kafka-chat-1.0.jar com.edmundophie.chat.ChatServer
	 ```
3. Run `ChatClient` from the generated `jar` in `target` folder  

	 ```
	 $ java -cp target/dependency/*:target/kafka-chat-1.0.jar com.edmundophie.chat.ChatClient
	 ```

## Chat Commands
- `nick <nickname>` : login as `nickname`. Leave `nickname` empty to login as a random user
- `join <channelname>` : join to a channel named `channelname`
- `leave <channelname>` : leave a channel named `channelname`
- `@<channelname> <message>` :  send `message` to a channel named `channelname`
- `<message>` : send a message to all user joined channel
- `logout` : logout from current `nickname`
- `exit` : stop program

## Testing
#### Conducted Testing:
* All basic commands (nick, join, leave, etc)
* Invalid command (e.g. `@<channelname>` without message)
* Send a message when no channel is joined
* Send a message to a channel that is not a member of
* Join a channel that already a member of
* Leave a channel that is not a member of
* Logout and relogin with the same nickname
* Login with existed nickname
* Multiclient chat

#### Testing Screenshoot:
![alt text](https://github.com/edmundophie/kafka-chat/blob/testing_screenshot_prak_5.png "Testing Result")

## Team Member
- Edmund Ophie 13512095
- Kevin 13512097

## [Github Link](https://github.com/edmundophie/kafka-chat.git) 
