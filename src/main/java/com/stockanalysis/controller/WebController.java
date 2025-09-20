package com.stockanalysis.controller;

import com.stockanalysis.service.StockDataService;
import com.stockanalysis.service.JobExecutionService;
import com.stockanalysis.model.JobExecution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.List;
import java.util.Map;

@Controller
public class WebController {

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
        try {
            // Get system stats
            Map<String, Object> systemStats = stockDataService.getSystemStats();
            
            // Get recent jobs (limit to 10)
            List<JobExecution> recentJobs = jobExecutionService.getRecentJobs(10);
            
            // Get last job execution
            JobExecution lastJob = jobExecutionService.getLastJobExecution();
            
            // Add data to model
            model.addAttribute("totalStocks", systemStats.get("availableSymbols"));
            model.addAttribute("totalDataPoints", systemStats.get("totalRecords"));
            model.addAttribute("daysAnalyzed", systemStats.get("daysAnalyzed"));
            model.addAttribute("lastJobStatus", lastJob != null ? lastJob.getStatus() : "Never Run");
            
            model.addAttribute("apiStatus", systemStats.get("authenticated").equals(true) ? "Connected" : "Disconnected");
            model.addAttribute("dbStatus", "Connected");
            model.addAttribute("schedulerStatus", systemStats.get("schedulerEnabled").equals(true) ? "Active" : "Inactive");
            model.addAttribute("dataFreshness", "Fresh");
            
            model.addAttribute("recentJobs", recentJobs);
            
            return "stock-dashboard";
        } catch (Exception e) {
            // If any error occurs, provide defaults
            model.addAttribute("totalStocks", 50);
            model.addAttribute("totalDataPoints", 0);
            model.addAttribute("daysAnalyzed", 0);
            model.addAttribute("lastJobStatus", "Error");
            model.addAttribute("apiStatus", "Unknown");
            model.addAttribute("dbStatus", "Unknown");
            model.addAttribute("schedulerStatus", "Unknown");
            model.addAttribute("dataFreshness", "Unknown");
            model.addAttribute("recentJobs", List.of());
            
            return "stock-dashboard";
        }
    }
    
    @GetMapping("/stock-analysis")
    public String analysis(Model model) {
        try {
            // Add basic data for the analysis page
            model.addAttribute("nifty50Stocks", stockDataService.getAvailableStockSymbols());
            model.addAttribute("stockData", List.of()); // Empty list initially
            
            return "stock-analysis";
        } catch (Exception e) {
            model.addAttribute("nifty50Stocks", List.of());
            model.addAttribute("stockData", List.of());
            return "stock-analysis";
        }
    }
}