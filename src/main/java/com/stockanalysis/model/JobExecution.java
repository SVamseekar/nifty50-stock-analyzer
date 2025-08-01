package com.stockanalysis.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.index.Indexed;
import java.time.LocalDateTime;

@Document(collection = "job_executions")
public class JobExecution {
    
    @Id
    private String id;
    
    @Indexed(unique = true)
    private String jobName;
    
    private LocalDateTime lastRun;
    private String status; // SUCCESS, FAILED, RUNNING
    private String message;
    
    // Constructors
    public JobExecution() {}
    
    public JobExecution(String jobName, LocalDateTime lastRun, String status) {
        this.jobName = jobName;
        this.lastRun = lastRun;
        this.status = status;
    }
    
    public JobExecution(String jobName, LocalDateTime lastRun, String status, String message) {
        this.jobName = jobName;
        this.lastRun = lastRun;
        this.status = status;
        this.message = message;
    }
    
    // Getters and Setters
    public String getId() {
        return id;
    }
    
    public void setId(String id) {
        this.id = id;
    }
    
    public String getJobName() {
        return jobName;
    }
    
    public void setJobName(String jobName) {
        this.jobName = jobName;
    }
    
    public LocalDateTime getLastRun() {
        return lastRun;
    }
    
    public void setLastRun(LocalDateTime lastRun) {
        this.lastRun = lastRun;
    }
    
    public String getStatus() {
        return status;
    }
    
    public void setStatus(String status) {
        this.status = status;
    }
    
    public String getMessage() {
        return message;
    }
    
    public void setMessage(String message) {
        this.message = message;
    }
    
    @Override
    public String toString() {
        return String.format("JobExecution{jobName='%s', lastRun=%s, status='%s', message='%s'}", 
                           jobName, lastRun, status, message);
    }
}