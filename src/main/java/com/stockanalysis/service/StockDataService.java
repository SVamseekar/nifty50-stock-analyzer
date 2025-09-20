package com.stockanalysis.service;

import com.stockanalysis.model.StockData;
import com.stockanalysis.repository.StockDataRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.scheduling.annotation.Scheduled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.JsonNode;

import java.time.LocalDate;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.stream.Collectors;

/**
 * FIXED: Enhanced Stock Data Service with proper Kite API integration
 * - Fixed data parsing from Kite API
 * - Better error handling and logging
 * - Improved data validation
 * - Optimized database operations
 */
@Service
public class StockDataService {
    
    private static final Logger logger = LoggerFactory.getLogger(StockDataService.class);
    
    @Autowired
    private KiteApiService kiteApiService;
    
    @Autowired
    private KiteAuthService kiteAuthService;
    
    @Autowired
    private StockDataRepository stockDataRepository;
    
    @Autowired
    private JobExecutionService jobExecutionService;
    
    @Autowired
    private MovingAverageService movingAverageService;
    
    @Value("${app.use.real.data:false}")
    private boolean useRealData;
    
    @Value("${app.mock.data.enabled:true}")
    private boolean mockDataEnabled;
    
    @Value("${app.nifty50.auto.fetch:true}")
    private boolean autoFetchEnabled;
    
    @Value("${app.scheduler.enabled:true}")
    private boolean schedulerEnabled;
    
    @Value("${app.moving.averages.enabled:true}")
    private boolean movingAveragesEnabled;
    
    // FIXED: Updated Nifty 50 stocks with verified instrument tokens
    private static final Map<String, String> NIFTY50_INSTRUMENTS = new HashMap<String, String>() {{
        put("RELIANCE", "738561");
        put("TCS", "2953217");
        put("HDFCBANK", "341249");
        put("INFY", "408065");
        put("ICICIBANK", "1270529");
        put("HINDUNILVR", "356865");
        put("ITC", "424961");
        put("SBIN", "779521");
        put("BHARTIARTL", "2714625");
        put("KOTAKBANK", "492033");
        put("ASIANPAINT", "60417");
        put("LT", "2939649");
        put("AXISBANK", "1510401");
        put("MARUTI", "2815745");
        put("SUNPHARMA", "857857");
        put("TITAN", "897537");
        put("ULTRACEMCO", "2952193");
        put("NESTLEIND", "4598529");
        put("WIPRO", "969473");
        put("HCLTECH", "1850625");
        put("BAJFINANCE", "81153");
        put("TECHM", "3465729");
        put("NTPC", "2977281");
        put("POWERGRID", "3834113");
        put("TATAMOTORS", "884737");
        put("COALINDIA", "5215745");
        put("TATASTEEL", "895745");
        put("BAJAJFINSV", "4268801");
        put("HDFCLIFE", "119553");
        put("ONGC", "633601");
        put("M&M", "519937");
        put("SBILIFE", "5582849");
        put("JSWSTEEL", "3001089");
        put("BRITANNIA", "140033");
        put("GRASIM", "315393");
        put("DRREDDY", "225537");
        put("CIPLA", "177665");
        put("EICHERMOT", "232961");
        put("ADANIENT", "6401");
        put("APOLLOHOSP", "41729");
        put("DIVISLAB", "2800641");
        put("INDUSINDBK", "1346049");
        put("HINDALCO", "348929");
        put("HEROMOTOCO", "345089");
        put("TATACONSUM", "878593");
        put("BPCL", "134657");
        put("LTIM", "11483906");
        put("ADANIPORTS", "3861249");
        put("UPL", "2889473");
        put("BAJAJ-AUTO", "78081");
    }};
    
    /**
     * ENHANCED: Scheduled job with better error handling
     */
    @Scheduled(cron = "0 0 19 * * MON-FRI", zone = "Asia/Kolkata")
    public void scheduledDataFetch() {
        if (!schedulerEnabled) {
            logger.info("Scheduler disabled, skipping scheduled data fetch");
            return;
        }
        
        logger.info("Starting scheduled daily data fetch at {}", LocalDate.now());
        
        try {
            Optional<LocalDate> lastRunDate = jobExecutionService.getLastSuccessfulRunDate("DAILY_STOCK_FETCH");
            LocalDate fromDate = lastRunDate.orElse(LocalDate.now().minusDays(5));
            LocalDate toDate = LocalDate.now();
            
            logger.info("Fetching data from {} to {}", fromDate, toDate);
            
            int recordsProcessed = fetchAllHistoricalData(fromDate, toDate);
            
            if (movingAveragesEnabled && recordsProcessed > 0) {
                logger.info("Calculating moving averages for recent data...");
                movingAverageService.calculateMovingAveragesForRecentData(fromDate);
            }
            
            logger.info("Scheduled job completed: {} records processed", recordsProcessed);
            
        } catch (Exception e) {
            logger.error("Scheduled job failed: {}", e.getMessage(), e);
            jobExecutionService.recordJobError("DAILY_STOCK_FETCH", e);
        }
    }
    
    /**
     * FIXED: Enhanced main data fetching method
     */
    public int fetchAllHistoricalData(LocalDate fromDate, LocalDate toDate) {
        String jobName = "DAILY_STOCK_FETCH_WITH_MA";
        jobExecutionService.recordJobStart(jobName);
        
        int totalRecordsProcessed = 0;
        int successfulStocks = 0;
        int failedStocks = 0;
        List<String> failedSymbols = new ArrayList<>();
        
        try {
            logger.info("Starting Nifty 50 data fetch for {} stocks from {} to {}", 
                       NIFTY50_INSTRUMENTS.size(), fromDate, toDate);
            
            // FIXED: Better authentication check
            if (!useRealData) {
                logger.warn("Real data disabled in configuration. Using mock data.");
                totalRecordsProcessed = generateMockData(fromDate, toDate);
            } else if (!kiteAuthService.isAuthenticated()) {
                logger.error("Kite API not authenticated. Please authenticate first.");
                logger.info("Visit: http://localhost:8081/stock-analyzer/api/auth/kite/login");
                totalRecordsProcessed = generateMockData(fromDate, toDate);
            } else {
                // Test connection first
                boolean connectionOk = kiteApiService.testConnection();
                if (!connectionOk) {
                    logger.error("Kite API connection test failed. Check authentication.");
                    throw new RuntimeException("Kite API connection failed");
                }
                
                logger.info("Kite API authenticated and connected. Fetching real data...");
                
                // Fetch real data from Kite API
                for (Map.Entry<String, String> entry : NIFTY50_INSTRUMENTS.entrySet()) {
                    String symbol = entry.getKey();
                    String instrumentToken = entry.getValue();
                    
                    try {
                        logger.debug("Processing {}: {}", symbol, instrumentToken);
                        
                        int recordsForSymbol = fetchAndProcessHistoricalData(symbol, instrumentToken, fromDate, toDate);
                        
                        if (recordsForSymbol > 0) {
                            totalRecordsProcessed += recordsForSymbol;
                            successfulStocks++;
                            logger.debug("{} - {} records processed", symbol, recordsForSymbol);
                        } else {
                            logger.warn("{} - No records processed", symbol);
                            failedStocks++;
                            failedSymbols.add(symbol);
                        }
                        
                        // Rate limiting
                        kiteApiService.rateLimit();
                        
                    } catch (Exception e) {
                        logger.error("Failed to process {}: {}", symbol, e.getMessage());
                        failedStocks++;
                        failedSymbols.add(symbol);
                    }
                }
            }
            
            // Calculate moving averages if enabled
            if (movingAveragesEnabled && totalRecordsProcessed > 0) {
                logger.info("Calculating moving averages for all updated stocks...");
                try {
                    movingAverageService.calculateMovingAveragesForAllStocks();
                    logger.info("Moving averages calculation completed");
                } catch (Exception e) {
                    logger.error("Moving averages calculation failed: {}", e.getMessage());
                }
            }
            
            String message = String.format(
                "Successfully processed %d stocks, %d failed. Total records: %d. Moving averages: %s. Failed symbols: %s", 
                successfulStocks, failedStocks, totalRecordsProcessed,
                movingAveragesEnabled ? "CALCULATED" : "DISABLED",
                failedSymbols.isEmpty() ? "None" : String.join(", ", failedSymbols)
            );
            
            jobExecutionService.recordJobCompletion(jobName, "SUCCESS", message);
            logger.info("Data fetch completed: {}", message);
            
            return totalRecordsProcessed;
            
        } catch (Exception e) {
            logger.error("Fatal error in data fetch job: {}", e.getMessage(), e);
            jobExecutionService.recordJobError(jobName, e);
            throw new RuntimeException("Data fetch job failed", e);
        }
    }
    
    /**
     * FIXED: Enhanced data fetching and processing for a single stock
     */
    private int fetchAndProcessHistoricalData(String symbol, String instrumentToken, 
                                            LocalDate fromDate, LocalDate toDate) {
        try {
            logger.debug("Fetching data for {}: {}", symbol, instrumentToken);
            
            // FIXED: Better validation
            if (!kiteApiService.isValidInstrumentToken(instrumentToken)) {
                logger.error("Invalid instrument token for {}: {}", symbol, instrumentToken);
                return 0;
            }
            
            // Fetch data from Kite API
            JsonNode historicalData = kiteApiService.fetchHistoricalData(instrumentToken, fromDate, toDate);
            
            if (historicalData == null) {
                logger.warn("No data received from Kite API for {}", symbol);
                return 0;
            }
            
            if (!historicalData.isArray() || historicalData.size() == 0) {
                logger.warn("Empty or invalid data array for {}", symbol);
                return 0;
            }
            
            logger.debug("Processing {} days of data for {}", historicalData.size(), symbol);
            
            int recordsProcessed = 0;
            List<StockData> stockDataList = new ArrayList<>();
            StockData previousDayData = null;
            
            // Process each day's data
            for (JsonNode dayData : historicalData) {
                try {
                    StockData stockData = parseKiteApiData(symbol, dayData);
                    
                    if (stockData != null) {
                        // Calculate percentage change from previous day
                        if (previousDayData != null) {
                            BigDecimal percentageChange = calculatePercentageChange(
                                previousDayData.getClosingPrice(), 
                                stockData.getClosingPrice()
                            );
                            stockData.setPercentageChange(percentageChange);
                        } else {
                            stockData.setPercentageChange(BigDecimal.ZERO);
                        }
                        
                        stockDataList.add(stockData);
                        previousDayData = stockData;
                        recordsProcessed++;
                    }
                    
                } catch (Exception e) {
                    logger.warn("Failed to parse day data for {}: {}", symbol, e.getMessage());
                }
            }
            
            // FIXED: Better database operations
            if (!stockDataList.isEmpty()) {
                try {
                    // Use upsert logic to avoid duplicates
                    for (StockData stockData : stockDataList) {
                        Optional<StockData> existing = stockDataRepository
                            .findBySymbolAndDate(stockData.getSymbol(), stockData.getDate());
                        
                        if (existing.isPresent()) {
                            // Update existing record
                            StockData existingData = existing.get();
                            existingData.setOpenPrice(stockData.getOpenPrice());
                            existingData.setHighPrice(stockData.getHighPrice());
                            existingData.setLowPrice(stockData.getLowPrice());
                            existingData.setClosingPrice(stockData.getClosingPrice());
                            existingData.setVolume(stockData.getVolume());
                            existingData.setPercentageChange(stockData.getPercentageChange());
                            existingData.setUpdatedAt(LocalDate.now());
                            stockDataRepository.save(existingData);
                        } else {
                            // Insert new record
                            stockDataRepository.save(stockData);
                        }
                    }
                    logger.debug("Saved/Updated {} records for {}", stockDataList.size(), symbol);
                } catch (Exception e) {
                    logger.error("Database error saving data for {}: {}", symbol, e.getMessage());
                    throw e;
                }
            }
            
            return recordsProcessed;
            
        } catch (Exception e) {
            logger.error("Error processing historical data for {}: {}", symbol, e.getMessage());
            throw new RuntimeException("Failed to process " + symbol, e);
        }
    }
    
    /**
     * FIXED: Enhanced Kite API data parsing with better error handling
     */
    private StockData parseKiteApiData(String symbol, JsonNode dayData) {
        try {
            // FIXED: Kite API returns array: [timestamp, open, high, low, close, volume]
            if (!dayData.isArray()) {
                logger.warn("Expected array format for {}, got: {}", symbol, dayData.getNodeType());
                return null;
            }
            
            if (dayData.size() < 6) {
                logger.warn("Invalid data array size for {}: expected 6 elements, got {}", 
                           symbol, dayData.size());
                logger.debug("Data content: {}", dayData);
                return null;
            }
            
            // FIXED: Parse timestamp correctly
            String timestamp = dayData.get(0).asText();
            LocalDate date;
            
            try {
                if (timestamp.contains("T")) {
                    // Format: "2024-08-21T00:00:00+0530"
                    date = LocalDate.parse(timestamp.substring(0, 10));
                } else if (timestamp.contains("-") && timestamp.length() >= 10) {
                    // Format: "2024-08-21"
                    date = LocalDate.parse(timestamp.substring(0, 10));
                } else {
                    logger.warn("Unrecognized timestamp format for {}: {}", symbol, timestamp);
                    return null;
                }
            } catch (Exception e) {
                logger.error("Failed to parse date for {}: {} - {}", symbol, timestamp, e.getMessage());
                return null;
            }
            
            // FIXED: Parse OHLCV data with proper validation
            BigDecimal openPrice, highPrice, lowPrice, closingPrice;
            Long volume;
            
            try {
                // Handle both string and numeric formats from API
                openPrice = parsePrice(dayData.get(1), "open", symbol);
                highPrice = parsePrice(dayData.get(2), "high", symbol);
                lowPrice = parsePrice(dayData.get(3), "low", symbol);
                closingPrice = parsePrice(dayData.get(4), "close", symbol);
                
                if (openPrice == null || highPrice == null || lowPrice == null || closingPrice == null) {
                    logger.warn("Invalid price data for {} on {}", symbol, date);
                    return null;
                }
                
                // Parse volume
                JsonNode volumeNode = dayData.get(5);
                if (volumeNode.isNull()) {
                    volume = 0L;
                } else {
                    volume = volumeNode.asLong();
                }
                
            } catch (Exception e) {
                logger.error("Failed to parse OHLCV data for {} on {}: {}", symbol, date, e.getMessage());
                return null;
            }
            
            // FIXED: Validate data consistency
            if (!isValidOHLCData(openPrice, highPrice, lowPrice, closingPrice)) {
                logger.warn("Invalid OHLC relationships for {} on {}: O={}, H={}, L={}, C={}", 
                           symbol, date, openPrice, highPrice, lowPrice, closingPrice);
                return null;
            }
            
            logger.debug("Parsed data for {} on {}: O={}, H={}, L={}, C={}, V={}", 
                        symbol, date, openPrice, highPrice, lowPrice, closingPrice, volume);
            
            // Create and return StockData object
            return new StockData(symbol, date, openPrice, highPrice, lowPrice, closingPrice, volume, BigDecimal.ZERO);
            
        } catch (Exception e) {
            logger.error("Unexpected error parsing Kite data for {}: {}", symbol, e.getMessage());
            return null;
        }
    }
    
    /**
     * FIXED: Helper method to parse price values safely
     */
    private BigDecimal parsePrice(JsonNode priceNode, String priceType, String symbol) {
        try {
            if (priceNode.isNull()) {
                logger.warn("Null {} price for {}", priceType, symbol);
                return null;
            }
            
            BigDecimal price;
            if (priceNode.isTextual()) {
                String priceText = priceNode.asText().trim();
                if (priceText.isEmpty()) {
                    logger.warn("Empty {} price for {}", priceType, symbol);
                    return null;
                }
                price = new BigDecimal(priceText);
            } else {
                price = BigDecimal.valueOf(priceNode.asDouble());
            }
            
            // Validate price is positive
            if (price.compareTo(BigDecimal.ZERO) <= 0) {
                logger.warn("Invalid {} price for {}: {}", priceType, symbol, price);
                return null;
            }
            
            return price.setScale(2, RoundingMode.HALF_UP);
            
        } catch (NumberFormatException e) {
            logger.error("Failed to parse {} price for {}: {}", priceType, symbol, priceNode.asText());
            return null;
        }
    }
    
    /**
     * FIXED: Validate OHLC data relationships
     */
    private boolean isValidOHLCData(BigDecimal open, BigDecimal high, BigDecimal low, BigDecimal close) {
        if (open == null || high == null || low == null || close == null) {
            return false;
        }
        
        // High should be >= all other prices
        if (high.compareTo(open) < 0 || high.compareTo(close) < 0 || high.compareTo(low) < 0) {
            return false;
        }
        
        // Low should be <= all other prices
        if (low.compareTo(open) > 0 || low.compareTo(close) > 0 || low.compareTo(high) > 0) {
            return false;
        }
        
        return true;
    }
    
    /**
     * Enhanced percentage change calculation
     */
    private BigDecimal calculatePercentageChange(BigDecimal previousPrice, BigDecimal currentPrice) {
        if (previousPrice == null || currentPrice == null || previousPrice.compareTo(BigDecimal.ZERO) == 0) {
            return BigDecimal.ZERO;
        }
        
        try {
            BigDecimal change = currentPrice.subtract(previousPrice);
            BigDecimal percentage = change.divide(previousPrice, 6, RoundingMode.HALF_UP)
                                         .multiply(new BigDecimal("100"))
                                         .setScale(2, RoundingMode.HALF_UP);
            
            return percentage;
        } catch (ArithmeticException e) {
            logger.warn("Failed to calculate percentage change: prev={}, curr={}", previousPrice, currentPrice);
            return BigDecimal.ZERO;
        }
    }
    
    /**
     * ENHANCED: Moving averages calculation trigger
     */
    public Map<String, Object> triggerMovingAveragesCalculation() {
        Map<String, Object> result = new HashMap<>();
        
        try {
            logger.info("Manual trigger: Calculating moving averages for all stocks");
            
            if (!movingAveragesEnabled) {
                result.put("status", "DISABLED");
                result.put("message", "Moving averages calculation is disabled");
                return result;
            }
            
            long startTime = System.currentTimeMillis();
            
            movingAverageService.calculateMovingAveragesForAllStocks();
            
            long duration = System.currentTimeMillis() - startTime;
            
            result.put("status", "SUCCESS");
            result.put("message", "Moving averages calculated for all stocks");
            result.put("durationMs", duration);
            result.put("timestamp", LocalDate.now());
            
            logger.info("Moving averages calculation completed in {}ms", duration);
            
        } catch (Exception e) {
            logger.error("Moving averages calculation failed: {}", e.getMessage(), e);
            result.put("status", "FAILED");
            result.put("error", e.getMessage());
        }
        
        return result;
    }
    
    /**
     * Get stock data with moving average filters
     */
    public List<StockData> getStockDataWithMAFilters(String symbol, LocalDate fromDate, 
                                                    LocalDate toDate, String maSignal100, 
                                                    String maSignal200, String goldenCross) {
        try {
            List<StockData> data = stockDataRepository.findBySymbolAndDateBetween(symbol, fromDate, toDate);
            
            return data.stream()
                .filter(stock -> {
                    if (maSignal100 != null && !maSignal100.equals("ALL")) {
                        if (!maSignal100.equals(stock.getMaSignal100())) {
                            return false;
                        }
                    }
                    
                    if (maSignal200 != null && !maSignal200.equals("ALL")) {
                        if (!maSignal200.equals(stock.getMaSignal200())) {
                            return false;
                        }
                    }
                    
                    if (goldenCross != null && !goldenCross.equals("ALL")) {
                        if (!goldenCross.equals(stock.getGoldenCross())) {
                            return false;
                        }
                    }
                    
                    return true;
                })
                .collect(Collectors.toList());
                
        } catch (Exception e) {
            logger.error("Error filtering stock data with MA filters: {}", e.getMessage());
            return new ArrayList<>();
        }
    }
    
    /**
     * Find stocks with golden cross signals
     */
    public Map<String, Object> findGoldenCrossStocks(LocalDate fromDate, LocalDate toDate) {
        Map<String, Object> result = new HashMap<>();
        
        try {
            List<Map<String, Object>> goldenCrossStocks = new ArrayList<>();
            
            for (String symbol : NIFTY50_INSTRUMENTS.keySet()) {
                List<StockData> goldenCrossData = getStockDataWithMAFilters(
                    symbol, fromDate, toDate, null, null, "GOLDEN_CROSS"
                );
                
                if (!goldenCrossData.isEmpty()) {
                    Map<String, Object> stockInfo = new HashMap<>();
                    stockInfo.put("symbol", symbol);
                    stockInfo.put("goldenCrossDays", goldenCrossData.size());
                    stockInfo.put("latestGoldenCross", goldenCrossData.stream()
                        .map(StockData::getDate)
                        .max(LocalDate::compareTo)
                        .orElse(null));
                    stockInfo.put("firstGoldenCross", goldenCrossData.stream()
                        .map(StockData::getDate)
                        .min(LocalDate::compareTo)
                        .orElse(null));
                    
                    goldenCrossStocks.add(stockInfo);
                }
            }
            
            result.put("status", "SUCCESS");
            result.put("fromDate", fromDate);
            result.put("toDate", toDate);
            result.put("goldenCrossStocks", goldenCrossStocks);
            result.put("totalStocks", goldenCrossStocks.size());
            result.put("timestamp", LocalDate.now());
            
        } catch (Exception e) {
            logger.error("Error finding golden cross stocks: {}", e.getMessage());
            result.put("status", "ERROR");
            result.put("error", e.getMessage());
        }
        
        return result;
    }
    
    /**
     * Get stock data with percentage change filter
     */
    public List<StockData> getStockDataWithPercentageFilter(String symbol, LocalDate fromDate, 
                                                           LocalDate toDate, Double minPercentage, 
                                                           Double maxPercentage) {
        try {
            if (minPercentage != null && maxPercentage != null) {
                return stockDataRepository.findBySymbolAndDateBetweenAndPercentageChangeBetween(
                    symbol, fromDate, toDate, 
                    BigDecimal.valueOf(minPercentage), BigDecimal.valueOf(maxPercentage)
                );
            } else if (minPercentage != null) {
                return stockDataRepository.findBySymbolAndDateBetweenAndPercentageChangeGreaterThanEqual(
                    symbol, fromDate, toDate, BigDecimal.valueOf(minPercentage)
                );
            } else if (maxPercentage != null) {
                return stockDataRepository.findBySymbolAndDateBetweenAndPercentageChangeLessThanEqual(
                    symbol, fromDate, toDate, BigDecimal.valueOf(maxPercentage)
                );
            } else {
                return stockDataRepository.findBySymbolAndDateBetween(symbol, fromDate, toDate);
            }
        } catch (Exception e) {
            logger.error("Error filtering stock data: {}", e.getMessage());
            return new ArrayList<>();
        }
    }
    
    /**
     * Get available stock symbols
     */
    public Set<String> getAvailableStockSymbols() {
        return NIFTY50_INSTRUMENTS.keySet();
    }
    
    /**
     * ENHANCED: Get system statistics with detailed information
     */
    public Map<String, Object> getSystemStats() {
        Map<String, Object> stats = new HashMap<>();
        
        try {
            long totalRecords = stockDataRepository.count();
            List<String> distinctSymbols = stockDataRepository.findDistinctSymbols();
            
            // Find date range
            Optional<StockData> latestRecord = stockDataRepository.findAll()
                .stream()
                .max(Comparator.comparing(StockData::getDate));
            
            Optional<StockData> earliestRecord = stockDataRepository.findAll()
                .stream()
                .min(Comparator.comparing(StockData::getDate));
            
            // Moving averages statistics
            long recordsWith100MA = 0;
            long recordsWith200MA = 0;
            long recordsWithGoldenCross = 0;
            
            if (movingAveragesEnabled) {
                for (String symbol : distinctSymbols) {
                    List<StockData> symbolData = stockDataRepository.findBySymbol(symbol);
                    recordsWith100MA += symbolData.stream()
                        .filter(d -> d.getMovingAverage100Day() != null)
                        .count();
                    recordsWith200MA += symbolData.stream()
                        .filter(d -> d.getMovingAverage200Day() != null)
                        .count();
                    recordsWithGoldenCross += symbolData.stream()
                        .filter(d -> "GOLDEN_CROSS".equals(d.getGoldenCross()))
                        .count();
                }
            }
            
            stats.put("totalRecords", totalRecords);
            stats.put("availableSymbols", distinctSymbols.size());
            stats.put("symbols", distinctSymbols);
            stats.put("nifty50Configured", NIFTY50_INSTRUMENTS.size());
            stats.put("useRealData", useRealData);
            stats.put("authenticated", kiteAuthService.isAuthenticated());
            stats.put("autoFetchEnabled", autoFetchEnabled);
            stats.put("schedulerEnabled", schedulerEnabled);
            stats.put("movingAveragesEnabled", movingAveragesEnabled);
            
            // API connection status
            if (kiteAuthService.isAuthenticated()) {
                stats.put("apiConnectionHealthy", kiteApiService.testConnection());
            } else {
                stats.put("apiConnectionHealthy", false);
            }
            
            // Moving averages stats
            stats.put("recordsWith100DayMA", recordsWith100MA);
            stats.put("recordsWith200DayMA", recordsWith200MA);
            stats.put("recordsWithGoldenCross", recordsWithGoldenCross);
            stats.put("ma100Coverage", totalRecords > 0 ? (recordsWith100MA * 100.0 / totalRecords) : 0);
            stats.put("ma200Coverage", totalRecords > 0 ? (recordsWith200MA * 100.0 / totalRecords) : 0);
            
            if (latestRecord.isPresent()) {
                stats.put("latestDataDate", latestRecord.get().getDate());
            }
            
            if (earliestRecord.isPresent()) {
                stats.put("earliestDataDate", earliestRecord.get().getDate());
            }
            
            stats.put("timestamp", java.time.LocalDateTime.now());
            
        } catch (Exception e) {
            logger.error("Error getting system stats: {}", e.getMessage());
            stats.put("error", "Failed to get stats: " + e.getMessage());
        }
        
        return stats;
    }
    
    /**
     * ENHANCED: Generate mock data with better validation
     */
    private int generateMockData(LocalDate fromDate, LocalDate toDate) {
        if (!mockDataEnabled) {
            logger.info("Mock data disabled, skipping data generation");
            return 0;
        }
        
        logger.info("Generating mock data for {} stocks from {} to {}", 
                   NIFTY50_INSTRUMENTS.size(), fromDate, toDate);
        
        Random random = new Random();
        int totalRecords = 0;
        
        for (String symbol : NIFTY50_INSTRUMENTS.keySet()) {
            BigDecimal basePrice = BigDecimal.valueOf(1000 + random.nextInt(4000));
            LocalDate currentDate = fromDate;
            
            while (!currentDate.isAfter(toDate)) {
                if (isWeekend(currentDate)) {
                    currentDate = currentDate.plusDays(1);
                    continue;
                }
                
                // Generate realistic OHLCV data
                BigDecimal volatility = BigDecimal.valueOf(0.02 + random.nextGaussian() * 0.01);
                BigDecimal change = basePrice.multiply(volatility);
                
                BigDecimal open = basePrice.add(change.multiply(BigDecimal.valueOf(random.nextGaussian() * 0.5)));
                BigDecimal close = open.add(change.multiply(BigDecimal.valueOf(random.nextGaussian())));
                BigDecimal high = close.max(open).add(change.multiply(BigDecimal.valueOf(Math.abs(random.nextGaussian() * 0.3))));
                BigDecimal low = close.min(open).subtract(change.multiply(BigDecimal.valueOf(Math.abs(random.nextGaussian() * 0.3))));
                
                Long volume = 100000L + random.nextInt(900000);
                BigDecimal percentageChange = calculatePercentageChange(basePrice, close);
                
                StockData stockData = new StockData(symbol, currentDate, 
                    open.setScale(2, RoundingMode.HALF_UP),
                    high.setScale(2, RoundingMode.HALF_UP),
                    low.setScale(2, RoundingMode.HALF_UP),
                    close.setScale(2, RoundingMode.HALF_UP),
                    volume, percentageChange);
                
                try {
                    // Check for existing data
                    if (!stockDataRepository.existsBySymbolAndDate(symbol, currentDate)) {
                        stockDataRepository.save(stockData);
                        totalRecords++;
                    }
                    basePrice = close;
                } catch (Exception e) {
                    logger.debug("Error saving mock data for {} on {}: {}", symbol, currentDate, e.getMessage());
                }
                
                currentDate = currentDate.plusDays(1);
            }
        }
        
        logger.info("Generated {} mock records", totalRecords);
        return totalRecords;
    }
    
    /**
     * Check if date is weekend
     */
    private boolean isWeekend(LocalDate date) {
        return date.getDayOfWeek().getValue() >= 6;
    }
    
    /**
     * Get Nifty 50 instrument token by symbol
     */
    public String getInstrumentToken(String symbol) {
        return NIFTY50_INSTRUMENTS.get(symbol.toUpperCase());
    }
    
    /**
     * Check if symbol is valid Nifty 50 stock
     */
    public boolean isValidNifty50Symbol(String symbol) {
        return NIFTY50_INSTRUMENTS.containsKey(symbol.toUpperCase());
    }

        /**
     * Get total count of data points
     */
    
        /**
     * Get total count of data points
     */
    public long getTotalDataPointsCount() {
        return stockDataRepository.count();
    } 

    /**
     * Get stock data after a specific date
     */
    public List<StockData> getStockDataAfterDate(LocalDate fromDate) {
        return stockDataRepository.findByDateGreaterThanEqual(fromDate);
    }

    /**
     * Get stock data by symbol and after date
     */
    public List<StockData> getStockDataBySymbolAndDateAfter(String symbol, LocalDate fromDate) {
        return stockDataRepository.findBySymbolAndDateGreaterThanEqual(symbol, fromDate);
    }

    /**
     * Get stock data with moving averages after date
     */
    public List<StockData> getStockDataWithMovingAveragesAfterDate(LocalDate fromDate) {
        return stockDataRepository.findByDateGreaterThanEqualAndMovingAverage100DayIsNotNull(fromDate);
    }

    /**
     * Get filtered stock data (enhanced version with all filters)
     */
    public List<StockData> getFilteredStockData(String symbol, LocalDate fromDate, LocalDate toDate, 
                                            BigDecimal minPercentage, BigDecimal maxPercentage, 
                                            Long minVolume, String sortBy, Integer limit) {
        
        // Start with basic query
        List<StockData> result = stockDataRepository.findByDateBetween(fromDate, toDate);
        
        // Apply filters using Java streams
        if (symbol != null && !symbol.isEmpty()) {
            result = result.stream()
                .filter(data -> symbol.equals(data.getSymbol()))
                .collect(Collectors.toList());
        }
        
        if (minPercentage != null) {
            result = result.stream()
                .filter(data -> data.getPercentageChange() != null && 
                            data.getPercentageChange().compareTo(minPercentage) >= 0)
                .collect(Collectors.toList());
        }
        
        if (maxPercentage != null) {
            result = result.stream()
                .filter(data -> data.getPercentageChange() != null && 
                            data.getPercentageChange().compareTo(maxPercentage) <= 0)
                .collect(Collectors.toList());
        }
        
        if (minVolume != null) {
            result = result.stream()
                .filter(data -> data.getVolume() != null && data.getVolume() >= minVolume)
                .collect(Collectors.toList());
        }
        
        // Apply sorting
        switch (sortBy) {
            case "percentageChange":
                result.sort((a, b) -> {
                    BigDecimal aChange = a.getPercentageChange() != null ? a.getPercentageChange() : BigDecimal.ZERO;
                    BigDecimal bChange = b.getPercentageChange() != null ? b.getPercentageChange() : BigDecimal.ZERO;
                    return bChange.compareTo(aChange); // Descending
                });
                break;
            case "volume":
                result.sort((a, b) -> {
                    Long aVolume = a.getVolume() != null ? a.getVolume() : 0L;
                    Long bVolume = b.getVolume() != null ? b.getVolume() : 0L;
                    return bVolume.compareTo(aVolume); // Descending
                });
                break;
            case "symbol":
                result.sort((a, b) -> a.getSymbol().compareTo(b.getSymbol()));
                break;
            case "date":
            default:
                result.sort((a, b) -> b.getDate().compareTo(a.getDate())); // Latest first
                break;
        }
        
        // Apply limit
        if (limit != null && limit > 0 && result.size() > limit) {
            result = result.subList(0, limit);
        }
        
        return result;
    }

    /**
     * Get stock data with percentage change greater than threshold
     */
    public List<StockData> getStockDataWithPercentageChangeGreaterThan(BigDecimal threshold, LocalDate fromDate) {
        return stockDataRepository.findByDateGreaterThanEqualAndPercentageChangeGreaterThanEqual(fromDate, threshold);
    }

    /**
     * Get stock data with percentage change less than threshold
     */
    public List<StockData> getStockDataWithPercentageChangeLessThan(BigDecimal threshold, LocalDate fromDate) {
        return stockDataRepository.findByDateGreaterThanEqualAndPercentageChangeLessThanEqual(fromDate, threshold);
    }

    /**
     * Get stock data with absolute percentage change greater than threshold
     */
    public List<StockData> getStockDataWithAbsolutePercentageChangeGreaterThan(BigDecimal threshold, LocalDate fromDate) {
        List<StockData> positiveChanges = getStockDataWithPercentageChangeGreaterThan(threshold, fromDate);
        List<StockData> negativeChanges = getStockDataWithPercentageChangeLessThan(threshold.negate(), fromDate);
        
        List<StockData> result = new ArrayList<>(positiveChanges);
        result.addAll(negativeChanges);
        
        return result.stream()
            .sorted((a, b) -> b.getDate().compareTo(a.getDate()))
            .collect(Collectors.toList());
    }

        /**
     * Get count of unique days analyzed
     */
    public long getDaysAnalyzedCount() {
        try {
            Long count = stockDataRepository.countDistinctDates();
            return count != null ? count : 0L;
        } catch (Exception e) {
            logger.warn("Could not get distinct dates count, using fallback calculation: {}", e.getMessage());
            // Fallback: estimate based on total records divided by number of stocks
            long totalRecords = stockDataRepository.count();
            int numberOfStocks = NIFTY50_INSTRUMENTS.size();
            return numberOfStocks > 0 ? Math.max(1, totalRecords / numberOfStocks) : 0L;
        }
    }
    /**
 * Get recent data for all stocks (for dashboard)
 */
    public List<StockData> getRecentDataForAllStocks(LocalDate fromDate, LocalDate toDate, Integer limit) {
        try {
            List<StockData> recentData = stockDataRepository.findByDateBetween(fromDate, toDate);
            
            // Sort by date descending (newest first)
            recentData.sort((a, b) -> b.getDate().compareTo(a.getDate()));
            
            // Apply limit if specified
            if (limit != null && limit > 0 && recentData.size() > limit) {
                recentData = recentData.subList(0, limit);
            }
            
            return recentData;
            
        } catch (Exception e) {
            logger.error("Error getting recent data for all stocks: {}", e.getMessage());
            return new ArrayList<>();
        }
    }

    /**
    * Check if Kite is authenticated (for dashboard status)
    */
    public boolean isKiteAuthenticated() {
        return kiteAuthService.isAuthenticated();
    }
}