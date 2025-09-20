package com.stockanalysis.repository;

import com.stockanalysis.model.JobExecution;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Repository
public interface JobExecutionRepository extends MongoRepository<JobExecution, String> {
    
    /**
     * Find job execution by job name
     */
    Optional<JobExecution> findByJobName(String jobName);
    
    /**
     * Find all job executions ordered by last run time (latest first)
     */
    List<JobExecution> findAllByOrderByLastRunDesc();
    
    
    /**
     * Find jobs that are currently running
     */
    @Query("{'status': 'RUNNING'}")
    List<JobExecution> findRunningJobs();
    
    /**
     * Find jobs that failed
     */
    @Query("{'status': 'FAILED'}")
    List<JobExecution> findFailedJobs();
    
    /**
     * Find jobs that succeeded
     */
    @Query("{'status': 'SUCCESS'}")
    List<JobExecution> findSuccessfulJobs();
    
    /**
     * Find jobs run after specific date
     */
    List<JobExecution> findByLastRunAfter(LocalDateTime dateTime);
    
    /**
     * Find jobs run before specific date (for cleanup)
     */
    List<JobExecution> findByLastRunBefore(LocalDateTime dateTime);
    
    /**
     * Delete old job executions before specific date
     */
    void deleteByLastRunBefore(LocalDateTime dateTime);
    
    /**
     * Count jobs by status
     */
    long countByStatus(String status);
    
    /**
     * Check if job exists by name
     */
    boolean existsByJobName(String jobName);
    
    /**
     * Find jobs that haven't run for specified days
     */
    @Query("{'lastRun': {$lt: ?0}}")
    List<JobExecution> findStaleJobs(LocalDateTime cutoffDate);
    
    /**
     * Get job execution history for a specific job (last N executions)
     */
    @Query("{'jobName': ?0}")
    List<JobExecution> findByJobNameOrderByLastRunDesc(String jobName);

        /**
     * Find top job by last run date
     */
    JobExecution findTopByOrderByLastRunDesc();

    /**
     * Find top 10 recent jobs
     */
    List<JobExecution> findTop10ByOrderByLastRunDesc();

    /**
     * Find jobs by status - KEEP ONLY ONE OF THESE
     */
    List<JobExecution> findByStatus(String status);

    /**
     * Find top job by started date
     */
    JobExecution findTopByOrderByStartedAtDesc();

    /**
     * Find top 10 recent jobs
     */
    List<JobExecution> findTop10ByOrderByStartedAtDesc();

    /**
     * Find all jobs ordered by date
     */
    List<JobExecution> findAllByOrderByStartedAtDesc();
    
    }