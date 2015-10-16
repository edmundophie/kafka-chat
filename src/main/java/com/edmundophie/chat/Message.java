package com.edmundophie.chat;

/**
 * Created by edmundophie on 9/18/15.
 */
public class Message implements Comparable<Message>{
    private String sender;
    private String text;
    private long timestamp;

    public Message() {
    }

    public Message(String sender, String text) {
        this.sender = sender;
        this.text = text;
        this.timestamp = System.currentTimeMillis();
    }

    public String getSender() {
        return sender;
    }

    public void setSender(String sender) {
        this.sender = sender;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public int compareTo(Message o) {
        return (this.timestamp<=o.getTimestamp())?-1:1;
    }
}
