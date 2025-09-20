package com.stockanalysis.controller;

import com.stockanalysis.service.StockDataService;
import com.stockanalysis.service.JobExecutionService;
import com.stockanalysis.model.JobExecution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

@Controller
public class WebController {
    
    private static final Logger logger = LoggerFactory.getLogger(WebController.class);

    @Autowired
    private StockDataService stockDataService;
    
    @Autowired
    private JobExecutionService jobExecutionService;

    @GetMapping("/test")
    @ResponseBody
    public String test() {
        return "WebController is working!";
    }
    
    @GetMapping({"/", "/dashboard"})
    public String dashboard(Model model) {
        logger.info("Loading dashboard page");
        
        // Initialize with safe defaults
        model.addAttribute("totalStocks", 50);
        model.addAttribute("totalDataPoints", 0L);
        model.addAttribute("daysAnalyzed", 0L);
        model.addAttribute("lastJobStatus", "Unknown");
        model.addAttribute("apiStatus", "Unknown");
        model.addAttribute("dbStatus", "Unknown");
        model.addAttribute("schedulerStatus", "Unknown");
        model.addAttribute("dataFreshness", "Unknown");
        model.addAttribute("recentJobs", List.of());
        
        try {
            // Get system stats with proper error handling
            Map<String, Object> systemStats = stockDataService.getSystemStats();
            if (systemStats != null && !systemStats.isEmpty()) {
                
                // Safe extraction with null checks
                Object availableSymbols = systemStats.get("availableSymbols");
                if (availableSymbols != null) {
                    model.addAttribute("totalStocks", availableSymbols);
                }
                
                Object totalRecords = systemStats.get("totalRecords");
                if (totalRecords != null) {
                    model.addAttribute("totalDataPoints", totalRecords);
                }
                
                // Safe boolean check
                Boolean authenticated = (Boolean) systemStats.get("authenticated");
                model.addAttribute("apiStatus", Boolean.TRUE.equals(authenticated) ? "Connected" : "Disconnected");
                
                Boolean schedulerEnabled = (Boolean) systemStats.get("schedulerEnabled");
                model.addAttribute("schedulerStatus", Boolean.TRUE.equals(schedulerEnabled) ? "Active" : "Inactive");
                
                model.addAttribute("dbStatus", "Connected");
                model.addAttribute("dataFreshness", "Fresh");
                
                logger.debug("System stats loaded successfully");
            } else {
                logger.warn("System stats returned null or empty");
            }
            
        } catch (Exception e) {
            logger.error("Failed to load system stats: {}", e.getMessage(), e);
            // Continue with defaults already set
        }
        
        try {
            // Get days analyzed with separate error handling
            long daysAnalyzed = stockDataService.getDaysAnalyzedCount();
            model.addAttribute("daysAnalyzed", daysAnalyzed);
            logger.debug("Days analyzed: {}", daysAnalyzed);
            
        } catch (Exception e) {
            logger.error("Failed to get days analyzed count: {}", e.getMessage(), e);
            // Keep default value of 0
        }
        
        try {
            // Get recent jobs with separate error handling
            List<JobExecution> recentJobs = jobExecutionService.getRecentJobs(10);
            if (recentJobs != null) {
                model.addAttribute("recentJobs", recentJobs);
                logger.debug("Loaded {} recent jobs", recentJobs.size());
            }
            
            // Get last job execution
            JobExecution lastJob = jobExecutionService.getLastJobExecution();
            if (lastJob != null && lastJob.getStatus() != null) {
                model.addAttribute("lastJobStatus", lastJob.getStatus());
            }
            
        } catch (Exception e) {
            logger.error("Failed to load job information: {}", e.getMessage(), e);
            // Continue with defaults
        }
        
        logger.info("Dashboard page loaded successfully");
        return "stock-dashboard";
    }
    
    @GetMapping("/stock-analysis")
    public String analysis(Model model) {
        logger.info("Loading stock analysis page");
        
        // Set safe defaults
        model.addAttribute("nifty50Stocks", List.of());
        model.addAttribute("stockData", List.of());
        
        try {
            // Get available symbols
            var availableSymbols = stockDataService.getAvailableStockSymbols();
            if (availableSymbols != null && !availableSymbols.isEmpty()) {
                model.addAttribute("nifty50Stocks", availableSymbols);
                logger.debug("Loaded {} available symbols", availableSymbols.size());
            } else {
                logger.warn("No available symbols found");
            }
            
        } catch (Exception e) {
            logger.error("Failed to load stock analysis data: {}", e.getMessage(), e);
            model.addAttribute("error", "Unable to load stock symbols. Please try again later.");
        }
        
        logger.info("Stock analysis page loaded");
        return "stock-analysis";
    }
}