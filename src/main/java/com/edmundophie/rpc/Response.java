package com.edmundophie.rpc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Created by edmundophie on 10/9/15.
 */
public class Response {
    private String status;
    private String message;
    private String nickname;

    public Response () {

    }

    public Response(String status, String message, String nickname) {
        this.status = status;
        this.message = message;
        this.nickname = nickname;
    }

    public Response(boolean status, String message, String nickname) {
        putStatus(status);
        this.message = message;
        this.nickname = nickname;
    }

    public Response(String status, String message) {
        this.status = status;
        this.message = message;
    }

    public String getStatus() {
        return status;
    }

    public boolean isStatus() { return status.equalsIgnoreCase("true"); }

    public void setStatus(String status) {
        this.status = status;
    }

    public void putStatus(boolean status) {
        this.status = status?"true":"false";
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }


    public String getNickname() {
        return nickname;
    }

    public void setNickname(String nickname) {
        this.nickname = nickname;
    }

    public String toString() {
        String json = null;
        try{
            json = new ObjectMapper().writeValueAsString(this);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        return json;
    }
}
