package com.edmundophie.chat;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by edmundophie on 9/17/15.
 */
public class User {
    private String nickname;
    private List<String> joinedChannel;
    private long logoutTimestamp;

    public User() {
        joinedChannel = new ArrayList<String>();
        logoutTimestamp = 0;
    }

    public User(String nickname) {
        this.nickname = nickname;
        joinedChannel = new ArrayList<String>();
        logoutTimestamp = 0;
    };

    public User(String nickname, List<String> joinedChannel, long logoutTimestamp) {
        this.nickname = nickname;
        this.joinedChannel = joinedChannel;
        this.logoutTimestamp = logoutTimestamp;
    };

    public long getLogoutTimestamp() {
        return logoutTimestamp;
    }

    public void setLogoutTimestamp(long logoutTimestamp) {
        this.logoutTimestamp = logoutTimestamp;
    }

    public List<String> getJoinedChannel() {
        return joinedChannel;
    }

    public void setJoinedChannel(List<String> joinedChannel) {
        this.joinedChannel = joinedChannel;
    }

    public String getNickname() {
        return nickname;
    }

    public void setNickname(String nickname) {
        this.nickname = nickname;
    }
}
