package com.bigdata.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.LocalDateTime;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class LogMessage {
    private String id;
    private String level;
    private String message;
    private String service;
    private String thread;
    private Object timestamp; // Changez en Object pour accepter String ou List
    private String stackTrace;
    private Boolean warning; // Ajoutez ces champs
    private Boolean critical;
    private Boolean error;

    @JsonCreator
    public LogMessage(
            @JsonProperty("id") String id,
            @JsonProperty("level") String level,
            @JsonProperty("message") String message,
            @JsonProperty("service") String service,
            @JsonProperty("thread") String thread,
            @JsonProperty("timestamp") Object timestamp,
            @JsonProperty("stackTrace") String stackTrace,
            @JsonProperty("warning") Boolean warning,
            @JsonProperty("critical") Boolean critical,
            @JsonProperty("error") Boolean error) {
        this.id = id;
        this.level = level;
        this.message = message;
        this.service = service;
        this.thread = thread;
        this.timestamp = timestamp;
        this.stackTrace = stackTrace;
        this.warning = warning;
        this.critical = critical;
        this.error = error;
    }


    // Getters
    public String getId() { return id; }
    public String getLevel() { return level; }
    public String getMessage() { return message; }
    public String getService() { return service; }
    public String getThread() { return thread; }
    public Object getTimestamp() { return timestamp; }
    public String getStackTrace() { return stackTrace; }
    public Boolean getWarning() { return warning; }
    public Boolean getCritical() { return critical; }
    public Boolean getError() { return error; }

    public boolean isErrorLevel() {
        return "ERROR".equals(level);
    }

    public boolean isCritical() {
        return isErrorLevel() && (message.contains("Exception") ||
                message.contains("Timeout") ||
                message.contains("OutOfMemory"));
    }

    // Méthode utilitaire pour obtenir le timestamp formaté
    public String getFormattedTimestamp() {
        if (timestamp instanceof List) {
            List<Integer> parts = (List<Integer>) timestamp;
            if (parts.size() >= 7) {
                return String.format("%04d-%02d-%02dT%02d:%02d:%02d.%d",
                        parts.get(0), parts.get(1), parts.get(2),
                        parts.get(3), parts.get(4), parts.get(5), parts.get(6));
            }
        }
        return timestamp.toString();
    }

    @Override
    public String toString() {
        return String.format("[%s] %s - %s - %s", level, getFormattedTimestamp(), service, message);
    }
}