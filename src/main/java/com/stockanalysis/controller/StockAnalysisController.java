package com.stockanalysis.controller;

import com.stockanalysis.model.StockData;
import com.stockanalysis.model.JobExecution;
import com.stockanalysis.service.StockDataService;
import com.stockanalysis.service.JobExecutionService;
import com.stockanalysis.service.KiteAuthService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.format.annotation.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.Optional;

@RestController
@RequestMapping("/api/stocks")
@CrossOrigin(origins = "*")
public class StockAnalysisController {
    
    private static final Logger logger = LoggerFactory.getLogger(StockAnalysisController.class);
    
    @Autowired
    private StockDataService stockDataService;
    
    @Autowired
    private JobExecutionService jobExecutionService;
    
    @Autowired
    private KiteAuthService kiteAuthService;
    
    /**
     * Health check endpoint
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> healthCheck() {
        Map<String, Object> health = new HashMap<>();
        health.put("status", "UP");
        health.put("timestamp", java.time.LocalDateTime.now());
        health.put("service", "Nifty 50 Stock Analysis API");
        health.put("version", "2.0.0");
        health.put("authenticated", kiteAuthService.isAuthenticated());
        health.put("realDataEnabled", kiteAuthService.isAuthenticated());
        
        return ResponseEntity.ok(health);
    }
    
    /**
     * Manual trigger endpoint for data fetching
     */
    @PostMapping("/trigger-job")
    public ResponseEntity<Map<String, Object>> triggerDataFetch(
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate fromDate,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate toDate) {
        
        Map<String, Object> response = new HashMap<>();
        
        try {
            if (fromDate == null) {
                fromDate = LocalDate.now().minusDays(7);
            }
            if (toDate == null) {
                toDate = LocalDate.now();
            }
            
            if (fromDate.isAfter(toDate)) {
                response.put("status", "ERROR");
                response.put("message", "fromDate cannot be after toDate");
                return ResponseEntity.badRequest().body(response);
            }
            
            logger.info("🚀 Manual job triggered for date range: {} to {}", fromDate, toDate);
            
            int recordsProcessed = stockDataService.fetchAllHistoricalData(fromDate, toDate);
            
            response.put("status", "SUCCESS");
            response.put("message", "Data fetch job completed successfully");
            response.put("recordsProcessed", recordsProcessed);
            response.put("fromDate", fromDate);
            response.put("toDate", toDate);
            response.put("timestamp", java.time.LocalDateTime.now());
            response.put("dataSource", kiteAuthService.isAuthenticated() ? "KITE_API" : "MOCK_DATA");
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("❌ Manual job trigger failed: {}", e.getMessage());
            
            response.put("status", "FAILED");
            response.put("message", "Job failed: " + e.getMessage());
            response.put("timestamp", java.time.LocalDateTime.now());
            
            return ResponseEntity.status(500).body(response);
        }
    }
    
    /**
     * Force trigger the scheduled job manually
     */
    @PostMapping("/trigger-scheduled-job")
    public ResponseEntity<Map<String, Object>> triggerScheduledJob() {
        Map<String, Object> response = new HashMap<>();
        
        try {
            logger.info("🕰️ Manually triggering scheduled job");
            stockDataService.scheduledDataFetch();
            
            response.put("status", "SUCCESS");
            response.put("message", "Scheduled job triggered successfully");
            response.put("timestamp", java.time.LocalDateTime.now());
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("❌ Manual scheduled job trigger failed: {}", e.getMessage());
            
            response.put("status", "FAILED");
            response.put("message", "Scheduled job failed: " + e.getMessage());
            response.put("timestamp", java.time.LocalDateTime.now());
            
            return ResponseEntity.status(500).body(response);
        }
    }
    
    /**
     * Get stock data for a specific symbol with percentage change filters
     */
    @GetMapping("/data/{symbol}")
    public ResponseEntity<Map<String, Object>> getStockDataWithFilter(
            @PathVariable String symbol,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate fromDate,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate toDate,
            @RequestParam(required = false) Double minPercentageChange,
            @RequestParam(required = false) Double maxPercentageChange) {
        
        try {
            if (!stockDataService.isValidNifty50Symbol(symbol)) {
                Map<String, Object> errorResponse = new HashMap<>();
                errorResponse.put("status", "ERROR");
                errorResponse.put("message", "Invalid Nifty 50 symbol: " + symbol);
                errorResponse.put("availableSymbols", stockDataService.getAvailableStockSymbols());
                return ResponseEntity.badRequest().body(errorResponse);
            }
            
            if (fromDate.isAfter(toDate)) {
                Map<String, Object> errorResponse = new HashMap<>();
                errorResponse.put("status", "ERROR");
                errorResponse.put("message", "fromDate cannot be after toDate");
                return ResponseEntity.badRequest().body(errorResponse);
            }
            
            logger.info("📊 Fetching data for symbol: {}, dates: {} to {}, percentage filter: [{}, {}]", 
                       symbol.toUpperCase(), fromDate, toDate, minPercentageChange, maxPercentageChange);
            
            List<StockData> stockData = stockDataService.getStockDataWithPercentageFilter(
                symbol.toUpperCase(), fromDate, toDate, minPercentageChange, maxPercentageChange);
            
            Map<String, Object> response = new HashMap<>();
            response.put("status", "SUCCESS");
            response.put("symbol", symbol.toUpperCase());
            response.put("fromDate", fromDate);
            response.put("toDate", toDate);
            response.put("totalRecords", stockData.size());
            response.put("filters", Map.of(
                "minPercentageChange", minPercentageChange != null ? minPercentageChange : "none",
                "maxPercentageChange", maxPercentageChange != null ? maxPercentageChange : "none"
            ));
            response.put("data", stockData);
            response.put("timestamp", java.time.LocalDateTime.now());
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("❌ Error fetching stock data for {}: {}", symbol, e.getMessage());
            
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("status", "ERROR");
            errorResponse.put("message", "Failed to fetch data: " + e.getMessage());
            errorResponse.put("symbol", symbol);
            
            return ResponseEntity.status(500).body(errorResponse);
        }
    }
    
    /**
     * Get system statistics
     */
    @GetMapping("/system/stats")
    public ResponseEntity<Map<String, Object>> getSystemStats() {
        try {
            Map<String, Object> stats = stockDataService.getSystemStats();
            return ResponseEntity.ok(stats);
        } catch (Exception e) {
            logger.error("❌ Error getting system stats: {}", e.getMessage());
            
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("status", "ERROR");
            errorResponse.put("message", "Failed to get system stats: " + e.getMessage());
            return ResponseEntity.status(500).body(errorResponse);
        }
    }
    
    /**
     * Get all available Nifty 50 stock symbols
     */
    @GetMapping("/symbols")
    public ResponseEntity<Map<String, Object>> getAvailableSymbols() {
        Set<String> symbols = stockDataService.getAvailableStockSymbols();
        
        Map<String, Object> response = new HashMap<>();
        response.put("status", "SUCCESS");
        response.put("symbols", symbols);
        response.put("count", symbols.size());
        response.put("type", "NIFTY_50_STOCKS");
        response.put("timestamp", java.time.LocalDateTime.now());
        
        return ResponseEntity.ok(response);
    }
    
    /**
     * Validate if a symbol is valid Nifty 50 stock
     */
    @GetMapping("/validate/{symbol}")
    public ResponseEntity<Map<String, Object>> validateSymbol(@PathVariable String symbol) {
        Map<String, Object> response = new HashMap<>();
        
        boolean isValid = stockDataService.isValidNifty50Symbol(symbol);
        String instrumentToken = stockDataService.getInstrumentToken(symbol);
        
        response.put("symbol", symbol.toUpperCase());
        response.put("isValid", isValid);
        response.put("isNifty50", isValid);
        
        if (isValid) {
            response.put("instrumentToken", instrumentToken);
            response.put("status", "VALID");
            response.put("message", "Valid Nifty 50 stock symbol");
        } else {
            response.put("status", "INVALID");
            response.put("message", "Invalid Nifty 50 stock symbol");
            response.put("availableSymbols", stockDataService.getAvailableStockSymbols());
        }
        
        response.put("timestamp", java.time.LocalDateTime.now());
        
        return ResponseEntity.ok(response);
    }
    
    /**
     * Job status endpoint - Get specific job status
     */
    @GetMapping("/job-status/{jobName}")
    public ResponseEntity<Map<String, Object>> getJobStatus(@PathVariable String jobName) {
        try {
            Optional<JobExecution> jobExecution = jobExecutionService.getJobStatus(jobName);
            
            if (jobExecution.isPresent()) {
                Map<String, Object> response = new HashMap<>();
                JobExecution job = jobExecution.get();
                
                response.put("status", "FOUND");
                response.put("jobName", job.getJobName());
                response.put("lastRun", job.getLastRun());
                response.put("jobStatus", job.getStatus());
                response.put("message", job.getMessage());
                response.put("isRunning", jobExecutionService.isJobRunning(jobName));
                response.put("timestamp", java.time.LocalDateTime.now());
                
                return ResponseEntity.ok(response);
            } else {
                Map<String, Object> response = new HashMap<>();
                response.put("status", "NOT_FOUND");
                response.put("message", "Job not found: " + jobName);
                response.put("jobName", jobName);
                
                return ResponseEntity.notFound().build();
            }
        } catch (Exception e) {
            logger.error("❌ Error getting job status for {}: {}", jobName, e.getMessage());
            
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("status", "ERROR");
            errorResponse.put("message", "Failed to get job status: " + e.getMessage());
            
            return ResponseEntity.status(500).body(errorResponse);
        }
    }
    
    /**
     * Get all job statuses
     */
    @GetMapping("/job-status")
    public ResponseEntity<Map<String, Object>> getAllJobStatuses() {
        try {
            List<JobExecution> jobs = jobExecutionService.getAllJobStatuses();
            
            Map<String, Object> response = new HashMap<>();
            response.put("status", "SUCCESS");
            response.put("totalJobs", jobs.size());
            response.put("jobs", jobs);
            response.put("timestamp", java.time.LocalDateTime.now());
            
            // Add summary statistics
            long runningJobs = jobs.stream().filter(j -> "RUNNING".equals(j.getStatus())).count();
            long successfulJobs = jobs.stream().filter(j -> "SUCCESS".equals(j.getStatus())).count();
            long failedJobs = jobs.stream().filter(j -> "FAILED".equals(j.getStatus())).count();
            
            Map<String, Object> summary = new HashMap<>();
            summary.put("running", runningJobs);
            summary.put("successful", successfulJobs);
            summary.put("failed", failedJobs);
            summary.put("total", jobs.size());
            
            response.put("summary", summary);
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("❌ Error getting all job statuses: {}", e.getMessage());
            
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("status", "ERROR");
            errorResponse.put("message", "Failed to get job statuses: " + e.getMessage());
            
            return ResponseEntity.status(500).body(errorResponse);
        }
    }
}