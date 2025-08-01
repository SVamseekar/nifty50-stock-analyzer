package com.stockanalysis.service;

import com.stockanalysis.model.JobExecution;
import com.stockanalysis.repository.JobExecutionRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Service
public class JobExecutionService {
    
    private static final Logger logger = LoggerFactory.getLogger(JobExecutionService.class);
    
    @Autowired
    private JobExecutionRepository jobExecutionRepository;
    
    /**
     * Record job start
     */
    public void recordJobStart(String jobName) {
        try {
            Optional<JobExecution> existingJob = jobExecutionRepository.findByJobName(jobName);
            
            JobExecution jobExecution;
            if (existingJob.isPresent()) {
                jobExecution = existingJob.get();
                jobExecution.setStatus("RUNNING");
                jobExecution.setLastRun(LocalDateTime.now());
                jobExecution.setMessage("Job started");
            } else {
                jobExecution = new JobExecution(jobName, LocalDateTime.now(), "RUNNING");
                jobExecution.setMessage("Job started");
            }
            
            jobExecutionRepository.save(jobExecution);
            logger.info("Job {} started at {}", jobName, jobExecution.getLastRun());
            
        } catch (Exception e) {
            logger.error("Failed to record job start for {}: {}", jobName, e.getMessage());
        }
    }
    
    /**
     * Record job completion
     */
    public void recordJobCompletion(String jobName, String status, String message) {
        try {
            Optional<JobExecution> existingJob = jobExecutionRepository.findByJobName(jobName);
            
            JobExecution jobExecution;
            if (existingJob.isPresent()) {
                jobExecution = existingJob.get();
                jobExecution.setStatus(status);
                jobExecution.setLastRun(LocalDateTime.now());
                jobExecution.setMessage(message);
            } else {
                jobExecution = new JobExecution(jobName, LocalDateTime.now(), status);
                jobExecution.setMessage(message);
            }
            
            jobExecutionRepository.save(jobExecution);
            logger.info("Job {} completed with status {} at {}: {}", jobName, status, jobExecution.getLastRun(), message);
            
        } catch (Exception e) {
            logger.error("Failed to record job completion for {}: {}", jobName, e.getMessage());
        }
    }
    
    /**
     * Record job error with detailed information
     */
    public void recordJobError(String jobName, Exception error) {
        try {
            String errorMessage = String.format("Job failed: %s", error.getMessage());
            recordJobCompletion(jobName, "FAILED", errorMessage);
            
            logger.error("Job {} failed with error: {}", jobName, error.getMessage(), error);
            
        } catch (Exception e) {
            logger.error("Failed to record job error for {}: {}", jobName, e.getMessage());
        }
    }
    
    /**
     * Get job status by name
     */
    public Optional<JobExecution> getJobStatus(String jobName) {
        try {
            return jobExecutionRepository.findByJobName(jobName);
        } catch (Exception e) {
            logger.error("Failed to get job status for {}: {}", jobName, e.getMessage());
            return Optional.empty();
        }
    }
    
    /**
     * Get all job statuses
     */
    public List<JobExecution> getAllJobStatuses() {
        try {
            return jobExecutionRepository.findAll();
        } catch (Exception e) {
            logger.error("Failed to get all job statuses: {}", e.getMessage());
            return List.of();
        }
    }
    
    /**
     * Get last successful run date - Enhanced for scheduling
     */
    public Optional<LocalDate> getLastSuccessfulRunDate(String jobName) {
        try {
            Optional<JobExecution> job = getJobStatus(jobName);
            if (job.isPresent() && "SUCCESS".equals(job.get().getStatus())) {
                return Optional.of(job.get().getLastRun().toLocalDate());
            }
            return Optional.empty();
        } catch (Exception e) {
            logger.error("Failed to get last successful run date for {}: {}", jobName, e.getMessage());
            return Optional.empty();
        }
    }
    
    /**
     * Check if job is currently running
     */
    public boolean isJobRunning(String jobName) {
        try {
            Optional<JobExecution> job = getJobStatus(jobName);
            return job.isPresent() && "RUNNING".equals(job.get().getStatus());
        } catch (Exception e) {
            logger.error("Failed to check if job is running for {}: {}", jobName, e.getMessage());
            return false;
        }
    }
    
    /**
     * Get job execution statistics
     */
    public JobExecutionStats getJobStats(String jobName) {
        try {
            Optional<JobExecution> job = getJobStatus(jobName);
            if (job.isPresent()) {
                JobExecution exec = job.get();
                return new JobExecutionStats(
                    exec.getJobName(),
                    exec.getStatus(),
                    exec.getLastRun(),
                    exec.getMessage(),
                    calculateJobHealth(exec)
                );
            }
            return new JobExecutionStats(jobName, "NEVER_RUN", null, "Job has never been executed", "UNKNOWN");
        } catch (Exception e) {
            logger.error("Failed to get job stats for {}: {}", jobName, e.getMessage());
            return new JobExecutionStats(jobName, "ERROR", null, "Failed to get stats", "ERROR");
        }
    }
    
    /**
     * Calculate job health based on execution history
     */
    private String calculateJobHealth(JobExecution jobExecution) {
        if (jobExecution.getLastRun() == null) {
            return "UNKNOWN";
        }
        
        LocalDateTime lastRun = jobExecution.getLastRun();
        LocalDateTime now = LocalDateTime.now();
        
        // If last run was successful and recent (within 24 hours), it's healthy
        if ("SUCCESS".equals(jobExecution.getStatus()) && 
            lastRun.isAfter(now.minusHours(24))) {
            return "HEALTHY";
        }
        
        // If last run failed, it's unhealthy
        if ("FAILED".equals(jobExecution.getStatus())) {
            return "UNHEALTHY";
        }
        
        // If job is running, it's active
        if ("RUNNING".equals(jobExecution.getStatus())) {
            return "ACTIVE";
        }
        
        // If last successful run was more than 24 hours ago, it's stale
        if (lastRun.isBefore(now.minusHours(24))) {
            return "STALE";
        }
        
        return "UNKNOWN";
    }
    
    /**
     * Clean up old job execution records (keep last 30 days)
     */
    public void cleanupOldExecutions() {
        try {
            LocalDateTime cutoffDate = LocalDateTime.now().minusDays(30);
            
            // Note: This would require a custom repository method
            // For now, just log the intent
            logger.info("Job execution cleanup would remove records older than {}", cutoffDate);
            
            // In real implementation, we can add:
            // jobExecutionRepository.deleteByLastRunBefore(cutoffDate);
            
        } catch (Exception e) {
            logger.error("Failed to cleanup old job executions: {}", e.getMessage());
        }
    }
    
    /**
     * Inner class for job execution statistics
     */
    public static class JobExecutionStats {
        private final String jobName;
        private final String status;
        private final LocalDateTime lastRun;
        private final String message;
        private final String health;
        
        public JobExecutionStats(String jobName, String status, LocalDateTime lastRun, String message, String health) {
            this.jobName = jobName;
            this.status = status;
            this.lastRun = lastRun;
            this.message = message;
            this.health = health;
        }
        
        // Getters
        public String getJobName() { return jobName; }
        public String getStatus() { return status; }
        public LocalDateTime getLastRun() { return lastRun; }
        public String getMessage() { return message; }
        public String getHealth() { return health; }
        
        @Override
        public String toString() {
            return String.format("JobExecutionStats{jobName='%s', status='%s', lastRun=%s, health='%s'}", 
                               jobName, status, lastRun, health);
        }
    }
}