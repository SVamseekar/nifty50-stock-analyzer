package com.stockanalysis.controller;

import com.stockanalysis.service.StockDataService;
import com.stockanalysis.service.JobExecutionService;
import com.stockanalysis.model.JobExecution;
import com.stockanalysis.model.StockData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.format.annotation.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Set;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

@Controller
public class WebController {
    
    private static final Logger logger = LoggerFactory.getLogger(WebController.class);

    @Autowired
    private StockDataService stockDataService;
    
    @Autowired
    private JobExecutionService jobExecutionService;

    /**
     * Main Dashboard - Stock Analysis Interface
     */
    @GetMapping({"/", "/dashboard"})
    public String dashboard(
            @RequestParam(required = false) String symbol,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate fromDate,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate toDate,
            @RequestParam(required = false) Double minPercentage,
            @RequestParam(required = false) Double maxPercentage,
            @RequestParam(required = false) Long minVolume,
            @RequestParam(required = false, defaultValue = "date") String sortBy,
            @RequestParam(required = false, defaultValue = "500") Integer limit,
            Model model) {
        
        logger.info("Loading main dashboard with filters");
        logger.info("Received filters - symbol: {}, fromDate: {}, toDate: {}, minPercentage: {}, maxPercentage: {}", 
                   symbol, fromDate, toDate, minPercentage, maxPercentage);
        
        // Set default date range if not provided
        if (fromDate == null) {
            fromDate = LocalDate.now().minusMonths(6);
        }
        if (toDate == null) {
            toDate = LocalDate.now();
        }
        
        // Add validation
        if (fromDate.isAfter(toDate)) {
            logger.warn("fromDate {} is after toDate {}, swapping them", fromDate, toDate);
            LocalDate temp = fromDate;
            fromDate = toDate;
            toDate = temp;
        }
        
        logger.info("Using date range: {} to {}", fromDate, toDate);
        
        try {
            // Get Nifty 50 symbols for dropdown
            Set<String> nifty50Stocks = stockDataService.getAvailableStockSymbols();
            model.addAttribute("nifty50Stocks", nifty50Stocks);
            
            // Get filtered stock data
            List<StockData> stockData = new ArrayList<>();
            
            if (symbol != null && !symbol.trim().isEmpty()) {
                // Filter by specific symbol
                logger.info("Filtering by symbol: {}", symbol.toUpperCase());
                stockData = stockDataService.getStockDataWithPercentageFilter(
                    symbol.toUpperCase(), fromDate, toDate, minPercentage, maxPercentage);
            } else {
                // Get recent data for all stocks FIRST
                stockData = stockDataService.getRecentDataForAllStocks(fromDate, toDate, limit);
                
                // THEN apply percentage filters manually if no symbol is selected
                if (minPercentage != null || maxPercentage != null) {
                    logger.info("Applying percentage filters: min={}, max={}", minPercentage, maxPercentage);
                    stockData = stockData.stream()
                        .filter(data -> {
                            if (data.getPercentageChange() == null) return false;
                            double change = data.getPercentageChange().doubleValue();
                            
                            if (minPercentage != null && change < minPercentage) return false;
                            if (maxPercentage != null && change > maxPercentage) return false;
                            
                            return true;
                        })
                        .collect(java.util.stream.Collectors.toList());
                }
            }
            
            logger.info("After initial filtering: {} records found", stockData.size());
            
            // Apply volume filter if specified
            if (minVolume != null) {
                logger.info("Applying volume filter: min volume={}", minVolume);
                stockData = stockData.stream()
                    .filter(data -> data.getVolume() != null && data.getVolume() >= minVolume)
                    .collect(java.util.stream.Collectors.toList());
                logger.info("After volume filtering: {} records found", stockData.size());
            }
            
            // Sort data
            stockData = sortStockData(stockData, sortBy);
            
            // Limit results
            if (limit > 0 && stockData.size() > limit) {
                stockData = stockData.subList(0, limit);
                logger.info("Limited results to {} records", limit);
            }
            
            // Calculate summary stats
            calculateSummaryStats(stockData, model);
            
            // Set model attributes
            model.addAttribute("stockData", stockData);
            model.addAttribute("selectedSymbol", symbol);
            model.addAttribute("fromDate", fromDate);
            model.addAttribute("toDate", toDate);
            model.addAttribute("minPercentage", minPercentage);
            model.addAttribute("maxPercentage", maxPercentage);
            model.addAttribute("minVolume", minVolume);
            model.addAttribute("sortBy", sortBy);
            model.addAttribute("limit", limit);
            
            // System status
            boolean authenticated = stockDataService.isKiteAuthenticated();
            model.addAttribute("kiteAuthenticated", authenticated);
            model.addAttribute("dataSource", authenticated ? "LIVE_KITE_API" : "HISTORICAL_DATA");
            
            logger.info("Dashboard loaded with {} final records", stockData.size());
            
        } catch (Exception e) {
            logger.error("Error loading dashboard: {}", e.getMessage());
            model.addAttribute("error", "Unable to load stock data: " + e.getMessage());
            model.addAttribute("stockData", List.of());
            model.addAttribute("nifty50Stocks", stockDataService.getAvailableStockSymbols());
        }
        
        return "main-dashboard";
    }

    private List<StockData> sortStockData(List<StockData> data, String sortBy) {
        logger.info("Sorting {} records by: {}", data.size(), sortBy);
        switch (sortBy) {
            case "percentageChange":
                return data.stream()
                    .sorted((a, b) -> {
                        java.math.BigDecimal aChange = a.getPercentageChange() != null ? a.getPercentageChange() : java.math.BigDecimal.ZERO;
                        java.math.BigDecimal bChange = b.getPercentageChange() != null ? b.getPercentageChange() : java.math.BigDecimal.ZERO;
                        return bChange.compareTo(aChange);
                    })
                    .collect(java.util.stream.Collectors.toList());
            case "volume":
                return data.stream()
                    .sorted((a, b) -> {
                        Long aVol = a.getVolume() != null ? a.getVolume() : 0L;
                        Long bVol = b.getVolume() != null ? b.getVolume() : 0L;
                        return bVol.compareTo(aVol);
                    })
                    .collect(java.util.stream.Collectors.toList());
            case "symbol":
                return data.stream()
                    .sorted((a, b) -> a.getSymbol().compareTo(b.getSymbol()))
                    .collect(java.util.stream.Collectors.toList());
            default: // date
                return data.stream()
                    .sorted((a, b) -> b.getDate().compareTo(a.getDate()))
                    .collect(java.util.stream.Collectors.toList());
        }
    }

    private void calculateSummaryStats(List<StockData> stockData, Model model) {
        if (stockData.isEmpty()) {
            model.addAttribute("positiveChanges", 0);
            model.addAttribute("negativeChanges", 0);
            model.addAttribute("avgPercentageChange", null);
            model.addAttribute("uniqueStocks", 0);
            model.addAttribute("dateRange", "No data");
            return;
        }
        
        long positiveChanges = stockData.stream()
            .filter(data -> data.getPercentageChange() != null && 
                        data.getPercentageChange().compareTo(java.math.BigDecimal.ZERO) > 0)
            .count();
        
        long negativeChanges = stockData.stream()
            .filter(data -> data.getPercentageChange() != null && 
                        data.getPercentageChange().compareTo(java.math.BigDecimal.ZERO) < 0)
            .count();
        
        java.util.OptionalDouble avgChange = stockData.stream()
            .filter(data -> data.getPercentageChange() != null)
            .mapToDouble(data -> data.getPercentageChange().doubleValue())
            .average();
        
        Set<String> uniqueSymbols = stockData.stream()
            .map(StockData::getSymbol)
            .collect(java.util.stream.Collectors.toSet());
        
        LocalDate minDate = stockData.stream()
            .map(StockData::getDate)
            .min(LocalDate::compareTo)
            .orElse(LocalDate.now());
        
        LocalDate maxDate = stockData.stream()
            .map(StockData::getDate)
            .max(LocalDate::compareTo)
            .orElse(LocalDate.now());
        
        model.addAttribute("positiveChanges", positiveChanges);
        model.addAttribute("negativeChanges", negativeChanges);
        model.addAttribute("avgPercentageChange", avgChange.isPresent() ? 
            java.math.BigDecimal.valueOf(avgChange.getAsDouble()).setScale(2, java.math.RoundingMode.HALF_UP) : null);
        model.addAttribute("uniqueStocks", uniqueSymbols.size());
        model.addAttribute("dateRange", minDate + " to " + maxDate);
    }
}