package com.stockanalysis.service;

import com.stockanalysis.model.StockData;
import com.stockanalysis.repository.StockDataRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Service to calculate Moving Averages for stock data
 * 
 * Key Points:
 * - 100-day MA needs 100 days of historical data
 * - 200-day MA needs 200 days of historical data
 * - Cannot calculate MA for initial period (first 100/200 days)
 */
@Service
public class MovingAverageService {
    
    private static final Logger logger = LoggerFactory.getLogger(MovingAverageService.class);
    
    @Autowired
    private StockDataRepository stockDataRepository;
    
    /**
     * Calculate and update moving averages for a specific stock
     * This method handles the "cannot calculate initial period" requirement
     */
    public void calculateMovingAveragesForStock(String symbol) {
        try {
            logger.info("🧮 Calculating moving averages for {}", symbol);
            
            // Get ALL historical data for the symbol, sorted by date ASC
            List<StockData> allData = stockDataRepository.findBySymbol(symbol)
                .stream()
                .sorted(Comparator.comparing(StockData::getDate))
                .collect(Collectors.toList());
            
            if (allData.size() < 100) {
                logger.warn("❌ {} has only {} days of data. Need at least 100 for MA calculation.", 
                           symbol, allData.size());
                return;
            }
            
            logger.info("📊 Processing {} days of data for {}", allData.size(), symbol);
            
            // Calculate moving averages
            List<StockData> updatedData = new ArrayList<>();
            
            for (int i = 0; i < allData.size(); i++) {
                StockData currentDay = allData.get(i);
                
                // Calculate 100-day MA (need at least 100 previous days)
                if (i >= 99) { // Index 99 = 100th day (0-based)
                    BigDecimal ma100 = calculate100DayMA(allData, i);
                    currentDay.setMovingAverage100Day(ma100);
                    
                    logger.debug("📈 {}: Day {} - 100-day MA = {}", 
                               symbol, i + 1, ma100);
                } else {
                    currentDay.setMovingAverage100Day(null); // Insufficient data
                    logger.debug("📊 {}: Day {} - Cannot calculate 100-day MA (need {} more days)", 
                               symbol, i + 1, 100 - (i + 1));
                }
                
                // Calculate 200-day MA (need at least 200 previous days)
                if (i >= 199) { // Index 199 = 200th day (0-based)
                    BigDecimal ma200 = calculate200DayMA(allData, i);
                    currentDay.setMovingAverage200Day(ma200);
                    
                    logger.debug("📈 {}: Day {} - 200-day MA = {}", 
                               symbol, i + 1, ma200);
                } else {
                    currentDay.setMovingAverage200Day(null); // Insufficient data
                    logger.debug("📊 {}: Day {} - Cannot calculate 200-day MA (need {} more days)", 
                               symbol, i + 1, 200 - (i + 1));
                }
                
                // Update timestamp
                currentDay.setUpdatedAt(LocalDate.now());
                updatedData.add(currentDay);
            }
            
            // Batch save all updated records
            stockDataRepository.saveAll(updatedData);
            
            // Log summary
            long records100MA = updatedData.stream()
                .filter(d -> d.getMovingAverage100Day() != null)
                .count();
            long records200MA = updatedData.stream()
                .filter(d -> d.getMovingAverage200Day() != null)
                .count();
            
            logger.info("✅ {} - Moving averages calculated:", symbol);
            logger.info("   📊 100-day MA: {} records", records100MA);
            logger.info("   📊 200-day MA: {} records", records200MA);
            logger.info("   📊 First 100-day MA date: {}", 
                       allData.size() >= 100 ? allData.get(99).getDate() : "N/A");
            logger.info("   📊 First 200-day MA date: {}", 
                       allData.size() >= 200 ? allData.get(199).getDate() : "N/A");
            
        } catch (Exception e) {
            logger.error("❌ Error calculating moving averages for {}: {}", symbol, e.getMessage(), e);
            throw new RuntimeException("Failed to calculate moving averages for " + symbol, e);
        }
    }
    
    /**
     * Calculate 100-day moving average for a specific day
     * Takes the average of closing prices for the current day and previous 99 days
     */
    private BigDecimal calculate100DayMA(List<StockData> data, int currentIndex) {
        if (currentIndex < 99) {
            return null; // Not enough data
        }
        
        BigDecimal sum = BigDecimal.ZERO;
        int count = 0;
        
        // Sum closing prices for current day + previous 99 days = 100 days total
        for (int i = currentIndex - 99; i <= currentIndex; i++) {
            BigDecimal closingPrice = data.get(i).getClosingPrice();
            if (closingPrice != null) {
                sum = sum.add(closingPrice);
                count++;
            }
        }
        
        if (count == 0) {
            return null;
        }
        
        return sum.divide(BigDecimal.valueOf(count), 2, RoundingMode.HALF_UP);
    }
    
    /**
     * Calculate 200-day moving average for a specific day
     * Takes the average of closing prices for the current day and previous 199 days
     */
    private BigDecimal calculate200DayMA(List<StockData> data, int currentIndex) {
        if (currentIndex < 199) {
            return null; // Not enough data
        }
        
        BigDecimal sum = BigDecimal.ZERO;
        int count = 0;
        
        // Sum closing prices for current day + previous 199 days = 200 days total
        for (int i = currentIndex - 199; i <= currentIndex; i++) {
            BigDecimal closingPrice = data.get(i).getClosingPrice();
            if (closingPrice != null) {
                sum = sum.add(closingPrice);
                count++;
            }
        }
        
        if (count == 0) {
            return null;
        }
        
        return sum.divide(BigDecimal.valueOf(count), 2, RoundingMode.HALF_UP);
    }
    
    /**
     * Calculate moving averages for all Nifty 50 stocks
     */
    public void calculateMovingAveragesForAllStocks() {
        logger.info("🚀 Starting moving average calculation for all stocks");
        
        try {
            // Get all distinct symbols
            List<String> symbols = stockDataRepository.findDistinctSymbols();
            
            logger.info("📊 Found {} stocks to process", symbols.size());
            
            int processed = 0;
            int errors = 0;
            
            for (String symbol : symbols) {
                try {
                    calculateMovingAveragesForStock(symbol);
                    processed++;
                    
                    // Progress logging
                    if (processed % 10 == 0) {
                        logger.info("🔄 Progress: {}/{} stocks processed", processed, symbols.size());
                    }
                    
                } catch (Exception e) {
                    logger.error("❌ Failed to process {}: {}", symbol, e.getMessage());
                    errors++;
                }
            }
            
            logger.info("✅ Moving average calculation completed!");
            logger.info("   📊 Successfully processed: {} stocks", processed);
            logger.info("   ❌ Errors: {} stocks", errors);
            
        } catch (Exception e) {
            logger.error("❌ Fatal error in moving average calculation: {}", e.getMessage(), e);
            throw new RuntimeException("Moving average calculation failed", e);
        }
    }
    
    /**
     * Calculate moving averages for stocks with data after a specific date
     * Useful for incremental updates
     */
    public void calculateMovingAveragesForRecentData(LocalDate fromDate) {
        logger.info("🔄 Calculating moving averages for data from {}", fromDate);
        
        try {
            // Get symbols that have data after the specified date
            List<String> symbols = stockDataRepository.findDistinctSymbols();
            
            int processed = 0;
            
            for (String symbol : symbols) {
                // Check if this symbol has recent data
                List<StockData> recentData = stockDataRepository
                    .findBySymbolAndDateBetween(symbol, fromDate, LocalDate.now());
                
                if (!recentData.isEmpty()) {
                    logger.info("🔄 Updating {} - {} recent records", symbol, recentData.size());
                    calculateMovingAveragesForStock(symbol);
                    processed++;
                }
            }
            
            logger.info("✅ Updated moving averages for {} stocks with recent data", processed);
            
        } catch (Exception e) {
            logger.error("❌ Error updating recent moving averages: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to update recent moving averages", e);
        }
    }
    
    /**
     * Get moving average statistics for a stock
     */
    public Map<String, Object> getMovingAverageStats(String symbol) {
        Map<String, Object> stats = new HashMap<>();
        
        try {
            List<StockData> data = stockDataRepository.findBySymbol(symbol);
            
            long totalRecords = data.size();
            long recordsWith100MA = data.stream()
                .filter(d -> d.getMovingAverage100Day() != null)
                .count();
            long recordsWith200MA = data.stream()
                .filter(d -> d.getMovingAverage200Day() != null)
                .count();
            
            // Find first MA dates
            Optional<LocalDate> first100MADate = data.stream()
                .filter(d -> d.getMovingAverage100Day() != null)
                .map(StockData::getDate)
                .min(LocalDate::compareTo);
            
            Optional<LocalDate> first200MADate = data.stream()
                .filter(d -> d.getMovingAverage200Day() != null)
                .map(StockData::getDate)
                .min(LocalDate::compareTo);
            
            stats.put("symbol", symbol);
            stats.put("totalRecords", totalRecords);
            stats.put("recordsWith100DayMA", recordsWith100MA);
            stats.put("recordsWith200DayMA", recordsWith200MA);
            stats.put("coverage100DayMA", totalRecords > 0 ? (recordsWith100MA * 100.0 / totalRecords) : 0);
            stats.put("coverage200DayMA", totalRecords > 0 ? (recordsWith200MA * 100.0 / totalRecords) : 0);
            stats.put("first100DayMADate", first100MADate.orElse(null));
            stats.put("first200DayMADate", first200MADate.orElse(null));
            stats.put("timestamp", LocalDate.now());
            
            return stats;
            
        } catch (Exception e) {
            logger.error("❌ Error getting MA stats for {}: {}", symbol, e.getMessage());
            stats.put("error", e.getMessage());
            return stats;
        }
    }
    
    /**
     * Check if a symbol has sufficient data for moving average calculation
     */
    public Map<String, Object> checkDataSufficiency(String symbol) {
        Map<String, Object> result = new HashMap<>();
        
        try {
            List<StockData> data = stockDataRepository.findBySymbol(symbol);
            
            int totalDays = data.size();
            boolean canCalculate100MA = totalDays >= 100;
            boolean canCalculate200MA = totalDays >= 200;
            
            result.put("symbol", symbol);
            result.put("totalDays", totalDays);
            result.put("canCalculate100DayMA", canCalculate100MA);
            result.put("canCalculate200DayMA", canCalculate200MA);
            result.put("daysNeededFor100MA", canCalculate100MA ? 0 : 100 - totalDays);
            result.put("daysNeededFor200MA", canCalculate200MA ? 0 : 200 - totalDays);
            
            if (totalDays > 0) {
                LocalDate earliestDate = data.stream()
                    .map(StockData::getDate)
                    .min(LocalDate::compareTo)
                    .orElse(null);
                
                LocalDate latestDate = data.stream()
                    .map(StockData::getDate)
                    .max(LocalDate::compareTo)
                    .orElse(null);
                
                result.put("earliestDate", earliestDate);
                result.put("latestDate", latestDate);
            }
            
            return result;
            
        } catch (Exception e) {
            logger.error("❌ Error checking data sufficiency for {}: {}", symbol, e.getMessage());
            result.put("error", e.getMessage());
            return result;
        }
    }
}