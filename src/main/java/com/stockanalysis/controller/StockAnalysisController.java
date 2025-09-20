package com.stockanalysis.controller;

import com.stockanalysis.model.StockData;
import com.stockanalysis.model.JobExecution;
import com.stockanalysis.service.StockDataService;
import com.stockanalysis.service.MovingAverageService;
import com.stockanalysis.service.JobExecutionService;
import com.stockanalysis.service.KiteAuthService;
import com.stockanalysis.service.KiteApiService;
import com.fasterxml.jackson.databind.JsonNode;
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
import java.util.Arrays;
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
    private JobExecutionService jobExecutionService;
    
    @Autowired
    private KiteAuthService kiteAuthService;
    
    @Autowired
    private KiteApiService kiteApiService;
    
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
     * Manual trigger for moving averages calculation
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
                
                logger.info("Manual trigger: Calculating moving averages for {}", symbol.toUpperCase());
                
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
                logger.info("Manual trigger: Calculating moving averages for ALL stocks");
                
                Map<String, Object> result = stockDataService.triggerMovingAveragesCalculation();
                response.putAll(result);
            }
            
            response.put("timestamp", java.time.LocalDateTime.now());
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("Moving averages calculation failed: {}", e.getMessage());
            
            response.put("status", "FAILED");
            response.put("message", "Moving averages calculation failed: " + e.getMessage());
            response.put("timestamp", java.time.LocalDateTime.now());
            
            return ResponseEntity.status(500).body(response);
        }
    }
    
    /**
     * Manual trigger for data fetching
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
            
            logger.info("Manual job triggered for date range: {} to {} (MA: {})", 
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
            response.put("dataSource", kiteAuthService.isAuthenticated() ? "KITE_API" : "NO_AUTH");
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
     * Get live Nifty 50 price - Enhanced version with fallback
     */
    @GetMapping("/nifty50-price")
    public ResponseEntity<Map<String, Object>> getNifty50Price() {
        Map<String, Object> response = new HashMap<>();
        
        try {
            if (!kiteAuthService.isAuthenticated()) {
                response.put("price", "Not Authenticated");
                response.put("change", "0.00");
                response.put("changePercent", "0.00");
                response.put("status", "NOT_AUTHENTICATED");
                return ResponseEntity.ok(response);
            }
            
            // Try to get Nifty 50 data from Kite API
            try {
                // Alternative approach - use different instrument format
                List<String> instruments = Arrays.asList("NSE:NIFTY 50");
                JsonNode quoteData = kiteApiService.fetchQuotes(instruments);
                
                if (quoteData != null && quoteData.has("NSE:NIFTY 50")) {
                    JsonNode niftyQuote = quoteData.get("NSE:NIFTY 50");
                    
                    double lastPrice = niftyQuote.has("last_price") ? niftyQuote.get("last_price").asDouble() : 0.0;
                    double netChange = niftyQuote.has("net_change") ? niftyQuote.get("net_change").asDouble() : 0.0;
                    double changePercent = niftyQuote.has("percentage_change") ? niftyQuote.get("percentage_change").asDouble() : 0.0;
                    
                    response.put("price", String.format("%.2f", lastPrice));
                    response.put("change", String.format("%.2f", netChange));
                    response.put("changePercent", String.format("%+.2f%%", changePercent));
                    response.put("status", "LIVE");
                    
                } else {
                    // Fallback: Use realistic sample data
                    response.put("price", "23,847.90");
                    response.put("change", "+127.45");
                    response.put("changePercent", "+0.54%");
                    response.put("status", "SAMPLE");
                }
                
            } catch (Exception apiError) {
                logger.warn("Kite API error for Nifty price: {}", apiError.getMessage());
                
                // Fallback data when API fails
                response.put("price", "23,847.90");
                response.put("change", "+127.45");
                response.put("changePercent", "+0.54%");
                response.put("status", "FALLBACK");
            }
            
            response.put("timestamp", java.time.LocalDateTime.now());
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("Error getting Nifty 50 price: {}", e.getMessage());
            
            response.put("price", "Market Closed");
            response.put("change", "0.00");
            response.put("changePercent", "0.00%");
            response.put("status", "ERROR");
            
            return ResponseEntity.ok(response);
        }
    }
}