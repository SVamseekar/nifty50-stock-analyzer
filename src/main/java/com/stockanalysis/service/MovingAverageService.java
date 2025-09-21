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
 * ENHANCED: Moving Average Service with Data Normalization Support
 * - Works with normalized data from StockDataService
 * - Handles mixed field formats transparently
 * - Calculates 50, 100, and 200-day moving averages
 * - Generates trading signals and golden cross detection
 */
@Service
public class MovingAverageService {
    
    private static final Logger logger = LoggerFactory.getLogger(MovingAverageService.class);
    
    @Autowired
    private StockDataRepository stockDataRepository;
    
    @Autowired
    private StockDataService stockDataService;
    
    /**
     * ENHANCED: Calculate and update moving averages for a specific stock using normalized data
     */
    public void calculateMovingAveragesForStock(String symbol) {
        try {
            logger.info("Calculating moving averages for {}", symbol);
            
            // CRITICAL: Use normalized data instead of raw repository data
            List<StockData> allData = stockDataService.getNormalizedDataForSymbol(symbol);
            
            if (allData.isEmpty()) {
                logger.warn("{} has no valid data after normalization", symbol);
                return;
            }
            
            if (allData.size() < 50) {
                logger.warn("{} has only {} days of normalized data. Need at least 50 for MA calculation.", 
                           symbol, allData.size());
                return;
            }
            
            logger.info("Processing {} days of normalized data for {}", allData.size(), symbol);
            
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
     * ENHANCED: Generic moving average calculation method
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
     * ENHANCED: Generate trading signal based on price vs moving average
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
     * ENHANCED: Calculate crossover signals (Golden Cross / Death Cross)
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
     * ENHANCED: Calculate overall trading signal strength
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
     * ENHANCED: Calculate moving averages for all stocks with better progress tracking
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
                    // Check if symbol has sufficient normalized data
                    List<StockData> normalizedData = stockDataService.getNormalizedDataForSymbol(symbol);
                    
                    if (normalizedData.size() >= 50) {
                        calculateMovingAveragesForStock(symbol);
                        hasValidData++;
                        logger.debug("Processed {} - {} normalized records", symbol, normalizedData.size());
                    } else {
                        logger.debug("Skipped {} - only {} normalized records (need 50+)", 
                                   symbol, normalizedData.size());
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
     * ENHANCED: Calculate moving averages for stocks with recent data
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
                    // Check if the symbol has enough total normalized data for MA calculation
                    List<StockData> normalizedData = stockDataService.getNormalizedDataForSymbol(symbol);
                    
                    if (normalizedData.size() >= 50) {
                        logger.info("Updating {} - {} recent records, {} total normalized", 
                                   symbol, recentData.size(), normalizedData.size());
                        calculateMovingAveragesForStock(symbol);
                        processed++;
                    } else {
                        logger.debug("Skipped {} - insufficient total data ({} records)", 
                                   symbol, normalizedData.size());
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
     * ENHANCED: Get moving average statistics for a stock with normalization info
     */
    public Map<String, Object> getMovingAverageStats(String symbol) {
        Map<String, Object> stats = new HashMap<>();
        
        try {
            // Get raw data count
            List<StockData> rawData = stockDataRepository.findBySymbol(symbol);
            
            // Get normalized data count
            List<StockData> normalizedData = stockDataService.getNormalizedDataForSymbol(symbol);
            
            long totalRawRecords = rawData.size();
            long totalNormalizedRecords = normalizedData.size();
            
            long recordsWith50MA = normalizedData.stream()
                .filter(d -> d.getMovingAverage50Day() != null)
                .count();
            long recordsWith100MA = normalizedData.stream()
                .filter(d -> d.getMovingAverage100Day() != null)
                .count();
            long recordsWith200MA = normalizedData.stream()
                .filter(d -> d.getMovingAverage200Day() != null)
                .count();
            
            // Find first MA dates
            Optional<LocalDate> first50MADate = normalizedData.stream()
                .filter(d -> d.getMovingAverage50Day() != null)
                .map(StockData::getDate)
                .min(LocalDate::compareTo);
            
            Optional<LocalDate> first100MADate = normalizedData.stream()
                .filter(d -> d.getMovingAverage100Day() != null)
                .map(StockData::getDate)
                .min(LocalDate::compareTo);
            
            Optional<LocalDate> first200MADate = normalizedData.stream()
                .filter(d -> d.getMovingAverage200Day() != null)
                .map(StockData::getDate)
                .min(LocalDate::compareTo);
            
            stats.put("symbol", symbol);
            stats.put("totalRawRecords", totalRawRecords);
            stats.put("totalNormalizedRecords", totalNormalizedRecords);
            stats.put("normalizationSuccess", totalNormalizedRecords > 0 ? 
                     (totalNormalizedRecords * 100.0 / totalRawRecords) : 0);
            
            stats.put("recordsWith50DayMA", recordsWith50MA);
            stats.put("recordsWith100DayMA", recordsWith100MA);
            stats.put("recordsWith200DayMA", recordsWith200MA);
            
            stats.put("coverage50DayMA", totalNormalizedRecords > 0 ? 
                     (recordsWith50MA * 100.0 / totalNormalizedRecords) : 0);
            stats.put("coverage100DayMA", totalNormalizedRecords > 0 ? 
                     (recordsWith100MA * 100.0 / totalNormalizedRecords) : 0);
            stats.put("coverage200DayMA", totalNormalizedRecords > 0 ? 
                     (recordsWith200MA * 100.0 / totalNormalizedRecords) : 0);
            
            stats.put("first50DayMADate", first50MADate.orElse(null));
            stats.put("first100DayMADate", first100MADate.orElse(null));
            stats.put("first200DayMADate", first200MADate.orElse(null));
            
            // Data sufficiency info
            stats.put("canCalculate50MA", totalNormalizedRecords >= 50);
            stats.put("canCalculate100MA", totalNormalizedRecords >= 100);
            stats.put("canCalculate200MA", totalNormalizedRecords >= 200);
            
            stats.put("timestamp", LocalDate.now());
            
            return stats;
            
        } catch (Exception e) {
            logger.error("Error getting MA stats for {}: {}", symbol, e.getMessage());
            stats.put("error", e.getMessage());
            return stats;
        }
    }
    
    /**
     * ENHANCED: Check data sufficiency with normalization info
     */
    public Map<String, Object> checkDataSufficiency(String symbol) {
        Map<String, Object> result = new HashMap<>();
        
        try {
            List<StockData> rawData = stockDataRepository.findBySymbol(symbol);
            List<StockData> normalizedData = stockDataService.getNormalizedDataForSymbol(symbol);
            
            int totalRawDays = rawData.size();
            int totalNormalizedDays = normalizedData.size();
            
            boolean canCalculate50MA = totalNormalizedDays >= 50;
            boolean canCalculate100MA = totalNormalizedDays >= 100;
            boolean canCalculate200MA = totalNormalizedDays >= 200;
            
            result.put("symbol", symbol);
            result.put("totalRawDays", totalRawDays);
            result.put("totalNormalizedDays", totalNormalizedDays);
            result.put("normalizationSuccess", totalRawDays > 0 ? 
                      (totalNormalizedDays * 100.0 / totalRawDays) : 0);
            
            result.put("canCalculate50DayMA", canCalculate50MA);
            result.put("canCalculate100DayMA", canCalculate100MA);
            result.put("canCalculate200DayMA", canCalculate200MA);
            
            result.put("daysNeededFor50MA", canCalculate50MA ? 0 : 50 - totalNormalizedDays);
            result.put("daysNeededFor100MA", canCalculate100MA ? 0 : 100 - totalNormalizedDays);
            result.put("daysNeededFor200MA", canCalculate200MA ? 0 : 200 - totalNormalizedDays);
            
            if (totalNormalizedDays > 0) {
                LocalDate earliestDate = normalizedData.stream()
                    .map(StockData::getDate)
                    .min(LocalDate::compareTo)
                    .orElse(null);
                
                LocalDate latestDate = normalizedData.stream()
                    .map(StockData::getDate)
                    .max(LocalDate::compareTo)
                    .orElse(null);
                
                result.put("earliestNormalizedDate", earliestDate);
                result.put("latestNormalizedDate", latestDate);
            }
            
            return result;
            
        } catch (Exception e) {
            logger.error("Error checking data sufficiency for {}: {}", symbol, e.getMessage());
            result.put("error", e.getMessage());
            return result;
        }
    }
    
    /**
     * NEW: Get stocks ready for moving average calculation
     */
    public Map<String, Object> getStocksReadyForMA() {
        Map<String, Object> result = new HashMap<>();
        
        try {
            List<String> symbols = stockDataRepository.findDistinctSymbols();
            
            List<String> readyFor50MA = new ArrayList<>();
            List<String> readyFor100MA = new ArrayList<>();
            List<String> readyFor200MA = new ArrayList<>();
            List<String> insufficientData = new ArrayList<>();
            
            for (String symbol : symbols) {
                List<StockData> normalizedData = stockDataService.getNormalizedDataForSymbol(symbol);
                int dataCount = normalizedData.size();
                
                if (dataCount >= 200) {
                    readyFor200MA.add(symbol);
                } else if (dataCount >= 100) {
                    readyFor100MA.add(symbol);
                } else if (dataCount >= 50) {
                    readyFor50MA.add(symbol);
                } else {
                    insufficientData.add(symbol);
                }
            }
            
            result.put("totalSymbols", symbols.size());
            result.put("readyFor50DayMA", readyFor50MA);
            result.put("readyFor100DayMA", readyFor100MA);
            result.put("readyFor200DayMA", readyFor200MA);
            result.put("insufficientData", insufficientData);
            
            result.put("readyFor50Count", readyFor50MA.size());
            result.put("readyFor100Count", readyFor100MA.size());
            result.put("readyFor200Count", readyFor200MA.size());
            result.put("insufficientCount", insufficientData.size());
            
            result.put("timestamp", LocalDate.now());
            
            logger.info("MA Readiness Summary: 50-day={}, 100-day={}, 200-day={}, insufficient={}", 
                       readyFor50MA.size(), readyFor100MA.size(), readyFor200MA.size(), insufficientData.size());
            
            return result;
            
        } catch (Exception e) {
            logger.error("Error getting stocks ready for MA: {}", e.getMessage());
            result.put("error", e.getMessage());
            return result;
        }
    }
}