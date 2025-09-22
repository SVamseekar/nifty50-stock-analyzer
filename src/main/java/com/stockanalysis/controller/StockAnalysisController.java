package com.stockanalysis.controller;

import com.stockanalysis.service.StockDataService;
import com.stockanalysis.service.KiteAuthService;
import com.stockanalysis.model.JobExecution;
import com.stockanalysis.service.JobExecutionService;
import com.stockanalysis.service.KiteApiService;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set; 
import java.util.Arrays;
import java.util.HashMap;

@RestController
@RequestMapping("/api/stocks")
@CrossOrigin(origins = "*")
public class StockAnalysisController {
    
    private static final Logger logger = LoggerFactory.getLogger(StockAnalysisController.class);
    
    @Autowired
    private StockDataService stockDataService;
    
        
    @Autowired
    private KiteAuthService kiteAuthService;
    
    @Autowired
    private KiteApiService kiteApiService;

    @Autowired
    private JobExecutionService jobExecutionService;
    
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
    /**
     * Get last job execution status for dashboard
     */
    @GetMapping("/jobs/last-execution")
    public ResponseEntity<Map<String, Object>> getLastJobExecution() {
        Map<String, Object> response = new HashMap<>();
        
        try {
            JobExecution lastJob = jobExecutionService.getLastJobExecution();
            
            if (lastJob != null) {
                response.put("jobName", lastJob.getJobName());
                response.put("lastRun", lastJob.getLastRun());
                response.put("status", lastJob.getStatus());
                response.put("message", lastJob.getMessage());
            } else {
                response.put("lastRun", null);
                response.put("status", "NEVER_RUN");
            }
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("Error getting last job execution: {}", e.getMessage());
            response.put("error", e.getMessage());
            return ResponseEntity.status(500).body(response);
        }
    }


}