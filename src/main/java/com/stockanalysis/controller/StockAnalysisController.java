package com.stockanalysis.controller;

import com.stockanalysis.model.StockData;
import com.stockanalysis.model.JobExecution;
import com.stockanalysis.service.StockDataService;
import com.stockanalysis.service.MovingAverageService;
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
    private MovingAverageService movingAverageService;
    
    @Autowired
    private JobExecutionService jobExecutionService;  // USED: Will be used in job status endpoints
    
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
        health.put("service", "Nifty 50 Stock Analysis API with Moving Averages");
        health.put("version", "3.0.0");
        health.put("authenticated", kiteAuthService.isAuthenticated());
        health.put("realDataEnabled", kiteAuthService.isAuthenticated());
        health.put("features", List.of("OHLCV Data", "Percentage Change", "100-day MA", "200-day MA", "Golden Cross"));
        
        return ResponseEntity.ok(health);
    }
    
    /**
     * NEW: Manual trigger for moving averages calculation
     * POST /api/stocks/trigger-moving-averages
     */
    @PostMapping("/trigger-moving-averages")
    public ResponseEntity<Map<String, Object>> triggerMovingAverages(
            @RequestParam(required = false) String symbol) {
        
        Map<String, Object> response = new HashMap<>();
        
        try {
            if (symbol != null && !symbol.trim().isEmpty()) {
                // Calculate for specific symbol
                if (!stockDataService.isValidNifty50Symbol(symbol)) {
                    response.put("status", "ERROR");
                    response.put("message", "Invalid Nifty 50 symbol: " + symbol);
                    return ResponseEntity.badRequest().body(response);
                }
                
                logger.info(" Manual trigger: Calculating moving averages for {}", symbol.toUpperCase());
                
                long startTime = System.currentTimeMillis();
                movingAverageService.calculateMovingAveragesForStock(symbol.toUpperCase());
                long duration = System.currentTimeMillis() - startTime;
                
                response.put("status", "SUCCESS");
                response.put("symbol", symbol.toUpperCase());
                response.put("message", "Moving averages calculated for " + symbol.toUpperCase());
                response.put("durationMs", duration);
                
                // Get stats for this symbol
                Map<String, Object> stats = movingAverageService.getMovingAverageStats(symbol.toUpperCase());
                response.put("stats", stats);
                
            } else {
                // Calculate for all symbols
                logger.info(" Manual trigger: Calculating moving averages for ALL stocks");
                
                Map<String, Object> result = stockDataService.triggerMovingAveragesCalculation();
                response.putAll(result);
            }
            
            response.put("timestamp", java.time.LocalDateTime.now());
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error(" Moving averages calculation failed: {}", e.getMessage());
            
            response.put("status", "FAILED");
            response.put("message", "Moving averages calculation failed: " + e.getMessage());
            response.put("timestamp", java.time.LocalDateTime.now());
            
            return ResponseEntity.status(500).body(response);
        }
    }
    
    /**
     * NEW: Get moving average statistics for a symbol
     * GET /api/stocks/moving-averages/stats/{symbol}
     */
    @GetMapping("/moving-averages/stats/{symbol}")
    public ResponseEntity<Map<String, Object>> getMovingAverageStats(@PathVariable String symbol) {
        try {
            if (!stockDataService.isValidNifty50Symbol(symbol)) {
                Map<String, Object> errorResponse = new HashMap<>();
                errorResponse.put("status", "ERROR");
                errorResponse.put("message", "Invalid Nifty 50 symbol: " + symbol);
                return ResponseEntity.badRequest().body(errorResponse);
            }
            
            Map<String, Object> stats = movingAverageService.getMovingAverageStats(symbol.toUpperCase());
            stats.put("status", "SUCCESS");
            
            return ResponseEntity.ok(stats);
            
        } catch (Exception e) {
            logger.error(" Error getting MA stats for {}: {}", symbol, e.getMessage());
            
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("status", "ERROR");
            errorResponse.put("message", "Failed to get moving average stats: " + e.getMessage());
            
            return ResponseEntity.status(500).body(errorResponse);
        }
    }
    
    /**
     * NEW: Check data sufficiency for moving averages
     * GET /api/stocks/moving-averages/sufficiency/{symbol}
     */
    @GetMapping("/moving-averages/sufficiency/{symbol}")
    public ResponseEntity<Map<String, Object>> checkDataSufficiency(@PathVariable String symbol) {
        try {
            if (!stockDataService.isValidNifty50Symbol(symbol)) {
                Map<String, Object> errorResponse = new HashMap<>();
                errorResponse.put("status", "ERROR");
                errorResponse.put("message", "Invalid Nifty 50 symbol: " + symbol);
                return ResponseEntity.badRequest().body(errorResponse);
            }
            
            Map<String, Object> sufficiency = movingAverageService.checkDataSufficiency(symbol.toUpperCase());
            sufficiency.put("status", "SUCCESS");
            
            return ResponseEntity.ok(sufficiency);
            
        } catch (Exception e) {
            logger.error(" Error checking data sufficiency for {}: {}", symbol, e.getMessage());
            
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("status", "ERROR");
            errorResponse.put("message", "Failed to check data sufficiency: " + e.getMessage());
            
            return ResponseEntity.status(500).body(errorResponse);
        }
    }
    
    /**
     * ENHANCED: Get stock data with moving average filters
     * GET /api/stocks/data-with-ma/{symbol}
     */
    @GetMapping("/data-with-ma/{symbol}")
    public ResponseEntity<Map<String, Object>> getStockDataWithMAFilters(
            @PathVariable String symbol,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate fromDate,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate toDate,
            @RequestParam(required = false) String maSignal100,
            @RequestParam(required = false) String maSignal200,
            @RequestParam(required = false) String goldenCross) {
        
        try {
            if (!stockDataService.isValidNifty50Symbol(symbol)) {
                Map<String, Object> errorResponse = new HashMap<>();
                errorResponse.put("status", "ERROR");
                errorResponse.put("message", "Invalid Nifty 50 symbol: " + symbol);
                return ResponseEntity.badRequest().body(errorResponse);
            }
            
            if (fromDate.isAfter(toDate)) {
                Map<String, Object> errorResponse = new HashMap<>();
                errorResponse.put("status", "ERROR");
                errorResponse.put("message", "fromDate cannot be after toDate");
                return ResponseEntity.badRequest().body(errorResponse);
            }
            
            logger.info(" Fetching data with MA filters for {}: {} to {}", 
                       symbol.toUpperCase(), fromDate, toDate);
            
            List<StockData> stockData = stockDataService.getStockDataWithMAFilters(
                symbol.toUpperCase(), fromDate, toDate, maSignal100, maSignal200, goldenCross);
            
            Map<String, Object> response = new HashMap<>();
            response.put("status", "SUCCESS");
            response.put("symbol", symbol.toUpperCase());
            response.put("fromDate", fromDate);
            response.put("toDate", toDate);
            response.put("totalRecords", stockData.size());
            response.put("filters", Map.of(
                "maSignal100", maSignal100 != null ? maSignal100 : "ALL",
                "maSignal200", maSignal200 != null ? maSignal200 : "ALL",
                "goldenCross", goldenCross != null ? goldenCross : "ALL"
            ));
            response.put("data", stockData);
            response.put("timestamp", java.time.LocalDateTime.now());
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error(" Error fetching stock data with MA filters for {}: {}", symbol, e.getMessage());
            
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("status", "ERROR");
            errorResponse.put("message", "Failed to fetch data: " + e.getMessage());
            
            return ResponseEntity.status(500).body(errorResponse);
        }
    }
    
    /**
     * NEW: Find stocks with golden cross signals
     * GET /api/stocks/golden-cross
     */
    @GetMapping("/golden-cross")
    public ResponseEntity<Map<String, Object>> findGoldenCrossStocks(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate fromDate,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate toDate) {
        
        try {
            if (fromDate.isAfter(toDate)) {
                Map<String, Object> errorResponse = new HashMap<>();
                errorResponse.put("status", "ERROR");
                errorResponse.put("message", "fromDate cannot be after toDate");
                return ResponseEntity.badRequest().body(errorResponse);
            }
            
            logger.info("🔍 Finding golden cross stocks from {} to {}", fromDate, toDate);
            
            Map<String, Object> result = stockDataService.findGoldenCrossStocks(fromDate, toDate);
            
            return ResponseEntity.ok(result);
            
        } catch (Exception e) {
            logger.error(" Error finding golden cross stocks: {}", e.getMessage());
            
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("status", "ERROR");
            errorResponse.put("message", "Failed to find golden cross stocks: " + e.getMessage());
            
            return ResponseEntity.status(500).body(errorResponse);
        }
    }
    
    /**
     * ENHANCED: Manual trigger for data fetching with moving averages
     * POST /api/stocks/trigger-job
     */
    @PostMapping("/trigger-job")
    public ResponseEntity<Map<String, Object>> triggerDataFetch(
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate fromDate,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate toDate,
            @RequestParam(required = false, defaultValue = "true") boolean calculateMovingAverages) {
        
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
            
            logger.info(" Manual job triggered for date range: {} to {} (MA: {})", 
                       fromDate, toDate, calculateMovingAverages);
            
            long startTime = System.currentTimeMillis();
            
            // Step 1: Fetch stock data
            int recordsProcessed = stockDataService.fetchAllHistoricalData(fromDate, toDate);
            
            long fetchDuration = System.currentTimeMillis() - startTime;
            
            // Step 2: Calculate moving averages if requested
            Map<String, Object> maResult = null;
            if (calculateMovingAverages && recordsProcessed > 0) {
                logger.info("Calculating moving averages...");
                maResult = stockDataService.triggerMovingAveragesCalculation();
            }
            
            long totalDuration = System.currentTimeMillis() - startTime;
            
            response.put("status", "SUCCESS");
            response.put("message", "Data fetch job completed successfully");
            response.put("recordsProcessed", recordsProcessed);
            response.put("fromDate", fromDate);
            response.put("toDate", toDate);
            response.put("fetchDurationMs", fetchDuration);
            response.put("totalDurationMs", totalDuration);
            response.put("dataSource", kiteAuthService.isAuthenticated() ? "KITE_API" : "MOCK_DATA");
            response.put("movingAveragesCalculated", calculateMovingAverages);
            
            if (maResult != null) {
                response.put("movingAveragesResult", maResult);
            }
            
            response.put("timestamp", java.time.LocalDateTime.now());
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("Manual job trigger failed: {}", e.getMessage());
            
            response.put("status", "FAILED");
            response.put("message", "Job failed: " + e.getMessage());
            response.put("timestamp", java.time.LocalDateTime.now());
            
            return ResponseEntity.status(500).body(response);
        }
    }
    
    /**
     * ENHANCED: Get system statistics with moving averages info
     * GET /api/stocks/system/stats
     */
    @GetMapping("/system/stats")
    public ResponseEntity<Map<String, Object>> getSystemStats() {
        try {
            Map<String, Object> stats = stockDataService.getSystemStats();
            return ResponseEntity.ok(stats);
        } catch (Exception e) {
            logger.error("Error getting system stats: {}", e.getMessage());
            
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("status", "ERROR");
            errorResponse.put("message", "Failed to get system stats: " + e.getMessage());
            return ResponseEntity.status(500).body(errorResponse);
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
            
            logger.info("Fetching data for symbol: {}, dates: {} to {}, percentage filter: [{}, {}]", 
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
            logger.error("Error fetching stock data for {}: {}", symbol, e.getMessage());
            
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("status", "ERROR");
            errorResponse.put("message", "Failed to fetch data: " + e.getMessage());
            errorResponse.put("symbol", symbol);
            
            return ResponseEntity.status(500).body(errorResponse);
        }
    }
    
    /**
     * Get all available Nifty 50 stock symbols
     */
    @GetMapping("/symbols")
    public ResponseEntity<Map<String, Object>> getAvailableSymbols() {
        Set<String> symbols = stockDataService.getAvailableStockSymbols();  // FIXED: Now Set is imported
        
        Map<String, Object> response = new HashMap<>();
        response.put("status", "SUCCESS");
        response.put("symbols", symbols);
        response.put("count", symbols.size());
        response.put("type", "NIFTY_50_STOCKS");
        response.put("timestamp", java.time.LocalDateTime.now());
        
        return ResponseEntity.ok(response);
    }
    
    /**
     * Force trigger the scheduled job manually
     */
    @PostMapping("/trigger-scheduled-job")
    public ResponseEntity<Map<String, Object>> triggerScheduledJob() {
        Map<String, Object> response = new HashMap<>();
        
        try {
            logger.info("Manually triggering scheduled job");
            stockDataService.scheduledDataFetch();
            
            response.put("status", "SUCCESS");
            response.put("message", "Scheduled job triggered successfully");
            response.put("timestamp", java.time.LocalDateTime.now());
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("Manual scheduled job trigger failed: {}", e.getMessage());
            
            response.put("status", "FAILED");
            response.put("message", "Scheduled job failed: " + e.getMessage());
            response.put("timestamp", java.time.LocalDateTime.now());
            
            return ResponseEntity.status(500).body(response);
        }
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
     * USING jobExecutionService here to fix the "unused field" warning
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
            logger.error("Error getting job status for {}: {}", jobName, e.getMessage());
            
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("status", "ERROR");
            errorResponse.put("message", "Failed to get job status: " + e.getMessage());
            
            return ResponseEntity.status(500).body(errorResponse);
        }
    }
    
    /**
     * Get all job statuses
     * USING jobExecutionService here to fix the "unused field" warning
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
            logger.error("Error getting all job statuses: {}", e.getMessage());
            
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("status", "ERROR");
            errorResponse.put("message", "Failed to get job statuses: " + e.getMessage());
            
            return ResponseEntity.status(500).body(errorResponse);
        }
    }
}