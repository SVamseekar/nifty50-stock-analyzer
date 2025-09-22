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
 * FIXED: Moving Average Service WITHOUT circular dependency
 * - Removed dependency on StockDataService
 * - Direct repository access for data retrieval
 * - Simplified data normalization logic
 */
@Service
public class MovingAverageService {
    
    private static final Logger logger = LoggerFactory.getLogger(MovingAverageService.class);
    
    @Autowired
    private StockDataRepository stockDataRepository;
    
    /**
     * FIXED: Calculate moving averages for a specific stock - NO CIRCULAR DEPENDENCY
     */
    public void calculateMovingAveragesForStock(String symbol) {
        try {
            logger.info("Calculating moving averages for {}", symbol);
            
            // Get all data directly from repository and sort by date
            List<StockData> allData = stockDataRepository.findBySymbol(symbol)
                .stream()
                .filter(this::hasValidClosingPrice)  // Filter out invalid data
                .sorted(Comparator.comparing(StockData::getDate))
                .collect(Collectors.toList());
            
            if (allData.isEmpty()) {
                logger.warn("{} has no valid data", symbol);
                return;
            }
            
            if (allData.size() < 50) {
                logger.warn("{} has only {} days of data. Need at least 50 for MA calculation.", 
                           symbol, allData.size());
                return;
            }
            
            logger.info("Processing {} days of data for {}", allData.size(), symbol);
            
            // Calculate moving averages
            List<StockData> updatedData = new ArrayList<>();
            
            for (int i = 0; i < allData.size(); i++) {
                StockData currentDay = allData.get(i);
                
                // Calculate 50-day MA (need at least 50 days)
                if (i >= 49) {
                    BigDecimal ma50 = calculateMovingAverage(allData, i, 50);
                    currentDay.setMovingAverage50Day(ma50);
                    
                    // Generate 50-day MA signal
                    if (ma50 != null && currentDay.getClosingPrice() != null) {
                        currentDay.setMaSignal50(generateMASignal(currentDay.getClosingPrice(), ma50));
                    }
                }
                
                // Calculate 100-day MA (need at least 100 days)
                if (i >= 99) {
                    BigDecimal ma100 = calculateMovingAverage(allData, i, 100);
                    currentDay.setMovingAverage100Day(ma100);
                    
                    // Generate 100-day MA signal
                    if (ma100 != null && currentDay.getClosingPrice() != null) {
                        currentDay.setMaSignal100(generateMASignal(currentDay.getClosingPrice(), ma100));
                    }
                    
                    logger.debug("{}: Day {} - 100-day MA = {}", symbol, i + 1, ma100);
                }
                
                // Calculate 200-day MA (need at least 200 days)
                if (i >= 199) {
                    BigDecimal ma200 = calculateMovingAverage(allData, i, 200);
                    currentDay.setMovingAverage200Day(ma200);
                    
                    // Generate 200-day MA signal
                    if (ma200 != null && currentDay.getClosingPrice() != null) {
                        currentDay.setMaSignal200(generateMASignal(currentDay.getClosingPrice(), ma200));
                    }
                    
                    logger.debug("{}: Day {} - 200-day MA = {}", symbol, i + 1, ma200);
                }
                
                // Calculate Golden Cross / Death Cross
                calculateCrossoverSignals(currentDay);
                
                // Update timestamp
                currentDay.setUpdatedAt(LocalDate.now());
                updatedData.add(currentDay);
            }
            
            // Batch save all updated records
            try {
                stockDataRepository.saveAll(updatedData);
                logger.debug("Saved {} updated records for {}", updatedData.size(), symbol);
            } catch (Exception e) {
                logger.error("Error saving MA data for {}: {}", symbol, e.getMessage());
                throw e;
            }
            
            // Log summary
            long records50MA = updatedData.stream()
                .filter(d -> d.getMovingAverage50Day() != null)
                .count();
            long records100MA = updatedData.stream()
                .filter(d -> d.getMovingAverage100Day() != null)
                .count();
            long records200MA = updatedData.stream()
                .filter(d -> d.getMovingAverage200Day() != null)
                .count();
            long goldenCrossEvents = updatedData.stream()
                .filter(d -> "GOLDEN_CROSS".equals(d.getGoldenCross()))
                .count();
            
            logger.info("{} - Moving averages calculated:", symbol);
            logger.info("   50-day MA: {} records", records50MA);
            logger.info("   100-day MA: {} records", records100MA);
            logger.info("   200-day MA: {} records", records200MA);
            logger.info("   Golden Cross events: {}", goldenCrossEvents);
            
            if (allData.size() >= 50) {
                logger.info("   First 50-day MA date: {}", allData.get(49).getDate());
            }
            if (allData.size() >= 100) {
                logger.info("   First 100-day MA date: {}", allData.get(99).getDate());
            }
            if (allData.size() >= 200) {
                logger.info("   First 200-day MA date: {}", allData.get(199).getDate());
            }
            
        } catch (Exception e) {
            logger.error("Error calculating moving averages for {}: {}", symbol, e.getMessage(), e);
            throw new RuntimeException("Failed to calculate moving averages for " + symbol, e);
        }
    }
    
    /**
     * Simple data validation - check if stock has valid closing price
     */
    private boolean hasValidClosingPrice(StockData stockData) {
        return stockData != null && 
               stockData.getClosingPrice() != null && 
               stockData.getClosingPrice().compareTo(BigDecimal.ZERO) > 0;
    }
    
    /**
     * Generic moving average calculation method
     */
    private BigDecimal calculateMovingAverage(List<StockData> data, int currentIndex, int period) {
        if (currentIndex < period - 1) {
            return null; // Not enough data
        }
        
        BigDecimal sum = BigDecimal.ZERO;
        int count = 0;
        
        // Sum closing prices for the specified period
        for (int i = currentIndex - period + 1; i <= currentIndex; i++) {
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
     * Generate trading signal based on price vs moving average
     */
    private String generateMASignal(BigDecimal currentPrice, BigDecimal movingAverage) {
        if (currentPrice == null || movingAverage == null) {
            return "INSUFFICIENT_DATA";
        }
        
        int comparison = currentPrice.compareTo(movingAverage);
        
        if (comparison > 0) {
            return "BUY";   // Price above MA (bullish)
        } else if (comparison < 0) {
            return "SELL";  // Price below MA (bearish)
        } else {
            return "HOLD";  // Price at MA (neutral)
        }
    }
    
    /**
     * Calculate crossover signals (Golden Cross / Death Cross)
     */
    private void calculateCrossoverSignals(StockData stockData) {
        BigDecimal ma50 = stockData.getMovingAverage50Day();
        BigDecimal ma100 = stockData.getMovingAverage100Day();
        BigDecimal ma200 = stockData.getMovingAverage200Day();
        
        // Primary crossover: 100-day vs 200-day MA
        if (ma100 != null && ma200 != null) {
            int comparison = ma100.compareTo(ma200);
            
            if (comparison > 0) {
                stockData.setGoldenCross("GOLDEN_CROSS");  // 100-day > 200-day (bullish)
            } else if (comparison < 0) {
                stockData.setGoldenCross("DEATH_CROSS");   // 100-day < 200-day (bearish)
            } else {
                stockData.setGoldenCross("NONE");          // Equal or very close
            }
        } else {
            stockData.setGoldenCross("INSUFFICIENT_DATA");
        }
        
        // Advanced signal: Combine multiple MAs for strength
        stockData.setTradingSignalStrength(calculateTradingSignalStrength(stockData));
    }
    
    /**
     * Calculate overall trading signal strength
     */
    private String calculateTradingSignalStrength(StockData stockData) {
        BigDecimal currentPrice = stockData.getClosingPrice();
        BigDecimal ma50 = stockData.getMovingAverage50Day();
        BigDecimal ma100 = stockData.getMovingAverage100Day();
        BigDecimal ma200 = stockData.getMovingAverage200Day();
        
        if (currentPrice == null) {
            return "INSUFFICIENT_DATA";
        }
        
        int bullishSignals = 0;
        int bearishSignals = 0;
        int totalSignals = 0;
        
        // Check 50-day MA signal
        if (ma50 != null) {
            totalSignals++;
            if (currentPrice.compareTo(ma50) > 0) {
                bullishSignals++;
            } else {
                bearishSignals++;
            }
        }
        
        // Check 100-day MA signal
        if (ma100 != null) {
            totalSignals++;
            if (currentPrice.compareTo(ma100) > 0) {
                bullishSignals++;
            } else {
                bearishSignals++;
            }
        }
        
        // Check 200-day MA signal
        if (ma200 != null) {
            totalSignals++;
            if (currentPrice.compareTo(ma200) > 0) {
                bullishSignals++;
            } else {
                bearishSignals++;
            }
        }
        
        if (totalSignals == 0) {
            return "INSUFFICIENT_DATA";
        }
        
        // Check for golden cross bonus
        boolean goldenCrossActive = "GOLDEN_CROSS".equals(stockData.getGoldenCross());
        boolean deathCrossActive = "DEATH_CROSS".equals(stockData.getGoldenCross());
        
        // Determine signal strength
        if (bullishSignals == totalSignals) {
            return goldenCrossActive ? "STRONG_BUY" : "BUY";
        } else if (bearishSignals == totalSignals) {
            return deathCrossActive ? "STRONG_SELL" : "SELL";
        } else {
            return "HOLD";
        }
    }
    
    /**
     * Calculate moving averages for all stocks with better progress tracking
     */
    public void calculateMovingAveragesForAllStocks() {
        logger.info("Starting moving average calculation for all stocks");
        
        try {
            // Get all distinct symbols
            List<String> symbols = stockDataRepository.findDistinctSymbols();
            
            logger.info("Found {} stocks to process", symbols.size());
            
            int processed = 0;
            int errors = 0;
            int hasValidData = 0;
            
            for (String symbol : symbols) {
                try {
                    // Check if symbol has sufficient data
                    long dataCount = stockDataRepository.countBySymbol(symbol);
                    
                    if (dataCount >= 50) {
                        calculateMovingAveragesForStock(symbol);
                        hasValidData++;
                        logger.debug("Processed {} - {} records", symbol, dataCount);
                    } else {
                        logger.debug("Skipped {} - only {} records (need 50+)", symbol, dataCount);
                    }
                    
                    processed++;
                    
                    // Progress logging
                    if (processed % 10 == 0) {
                        logger.info("Progress: {}/{} stocks processed, {} had sufficient data", 
                                   processed, symbols.size(), hasValidData);
                    }
                    
                } catch (Exception e) {
                    logger.error("Failed to process {}: {}", symbol, e.getMessage());
                    errors++;
                }
            }
            
            logger.info("Moving average calculation completed!");
            logger.info("   Successfully processed: {} stocks", processed);
            logger.info("   Had sufficient data: {} stocks", hasValidData);
            logger.info("   Errors: {} stocks", errors);
            
        } catch (Exception e) {
            logger.error("Fatal error in moving average calculation: {}", e.getMessage(), e);
            throw new RuntimeException("Moving average calculation failed", e);
        }
    }
    
    /**
     * Calculate moving averages for stocks with recent data
     */
    public void calculateMovingAveragesForRecentData(LocalDate fromDate) {
        logger.info("Calculating moving averages for data from {}", fromDate);
        
        try {
            List<String> symbols = stockDataRepository.findDistinctSymbols();
            
            int processed = 0;
            
            for (String symbol : symbols) {
                // Check if this symbol has recent data
                List<StockData> recentData = stockDataRepository
                    .findBySymbolAndDateBetween(symbol, fromDate, LocalDate.now());
                
                if (!recentData.isEmpty()) {
                    // Check if the symbol has enough total data for MA calculation
                    long totalData = stockDataRepository.countBySymbol(symbol);
                    
                    if (totalData >= 50) {
                        logger.info("Updating {} - {} recent records, {} total", 
                                   symbol, recentData.size(), totalData);
                        calculateMovingAveragesForStock(symbol);
                        processed++;
                    } else {
                        logger.debug("Skipped {} - insufficient total data ({} records)", 
                                   symbol, totalData);
                    }
                }
            }
            
            logger.info("Updated moving averages for {} stocks with recent data", processed);
            
        } catch (Exception e) {
            logger.error("Error updating recent moving averages: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to update recent moving averages", e);
        }
    }
    
    /**
     * Get moving average statistics for a stock
     */
    public Map<String, Object> getMovingAverageStats(String symbol) {
        Map<String, Object> stats = new HashMap<>();
        
        try {
            List<StockData> symbolData = stockDataRepository.findBySymbol(symbol);
            
            long totalRecords = symbolData.size();
            
            long recordsWith50MA = symbolData.stream()
                .filter(d -> d.getMovingAverage50Day() != null)
                .count();
            long recordsWith100MA = symbolData.stream()
                .filter(d -> d.getMovingAverage100Day() != null)
                .count();
            long recordsWith200MA = symbolData.stream()
                .filter(d -> d.getMovingAverage200Day() != null)
                .count();
            
            // Find first MA dates
            Optional<LocalDate> first50MADate = symbolData.stream()
                .filter(d -> d.getMovingAverage50Day() != null)
                .map(StockData::getDate)
                .min(LocalDate::compareTo);
            
            Optional<LocalDate> first100MADate = symbolData.stream()
                .filter(d -> d.getMovingAverage100Day() != null)
                .map(StockData::getDate)
                .min(LocalDate::compareTo);
            
            Optional<LocalDate> first200MADate = symbolData.stream()
                .filter(d -> d.getMovingAverage200Day() != null)
                .map(StockData::getDate)
                .min(LocalDate::compareTo);
            
            stats.put("symbol", symbol);
            stats.put("totalRecords", totalRecords);
            
            stats.put("recordsWith50DayMA", recordsWith50MA);
            stats.put("recordsWith100DayMA", recordsWith100MA);
            stats.put("recordsWith200DayMA", recordsWith200MA);
            
            stats.put("coverage50DayMA", totalRecords > 0 ? 
                     (recordsWith50MA * 100.0 / totalRecords) : 0);
            stats.put("coverage100DayMA", totalRecords > 0 ? 
                     (recordsWith100MA * 100.0 / totalRecords) : 0);
            stats.put("coverage200DayMA", totalRecords > 0 ? 
                     (recordsWith200MA * 100.0 / totalRecords) : 0);
            
            stats.put("first50DayMADate", first50MADate.orElse(null));
            stats.put("first100DayMADate", first100MADate.orElse(null));
            stats.put("first200DayMADate", first200MADate.orElse(null));
            
            // Data sufficiency info
            stats.put("canCalculate50MA", totalRecords >= 50);
            stats.put("canCalculate100MA", totalRecords >= 100);
            stats.put("canCalculate200MA", totalRecords >= 200);
            
            stats.put("timestamp", LocalDate.now());
            
            return stats;
            
        } catch (Exception e) {
            logger.error("Error getting MA stats for {}: {}", symbol, e.getMessage());
            stats.put("error", e.getMessage());
            return stats;
        }
    }
}