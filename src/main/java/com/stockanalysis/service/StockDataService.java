package com.stockanalysis.service;

import com.stockanalysis.model.StockData;
import com.stockanalysis.repository.StockDataRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.JsonNode;
import org.bson.Document;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.stream.Collectors;

/**
 * ENHANCED: Stock Data Service with Data Normalization
 * - Handles both snake_case and camelCase MongoDB fields
 * - Normalizes data on retrieval for consistent processing
 * - Supports moving averages calculation on normalized data
 */
@Service
public class StockDataService {
    
    private static final Logger logger = LoggerFactory.getLogger(StockDataService.class);
    
    private static final Set<String> INDIAN_MARKET_HOLIDAYS = Set.of(
        "2025-01-26", // Republic Day
        "2025-03-14", // Holi
        "2025-04-18", // Good Friday
        "2025-05-01", // Maharashtra Day
        "2025-08-15", // Independence Day
        "2025-10-02", // Gandhi Jayanti
        "2025-10-20", // Dussehra
        "2025-11-01", // Diwali
        "2025-11-14", // Guru Nanak Jayanti
        "2025-12-25"  // Christmas
    );


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
    
    @Autowired
    private MongoTemplate mongoTemplate;
    
    @Value("${app.use.real.data:false}")
    private boolean useRealData;
    
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
     * CORE: Data normalization method to handle mixed field formats
     * Converts snake_case MongoDB fields to camelCase Java model fields
     */
    private StockData normalizeStockData(StockData stockData) {
        if (stockData == null) {
            return null;
        }
        
        try {
            // Query MongoDB directly to get raw document
            Query query = new Query(Criteria.where("symbol").is(stockData.getSymbol())
                                  .and("date").is(stockData.getDate()));
            
            Document rawDoc = mongoTemplate.findOne(query, Document.class, "stock_data");
            
            if (rawDoc != null) {
                // Handle snake_case to camelCase conversion
                if (stockData.getClosingPrice() == null && rawDoc.containsKey("closing_price")) {
                    Object closingPriceObj = rawDoc.get("closing_price");
                    if (closingPriceObj != null && !"N/A".equals(closingPriceObj.toString())) {
                        stockData.setClosingPrice(parseStringOrNumericPrice(closingPriceObj));
                    }
                }
                
                if (stockData.getOpenPrice() == null && rawDoc.containsKey("open_price")) {
                    Object openPriceObj = rawDoc.get("open_price");
                    if (openPriceObj != null && !"N/A".equals(openPriceObj.toString())) {
                        stockData.setOpenPrice(parseStringOrNumericPrice(openPriceObj));
                    }
                }
                
                if (stockData.getHighPrice() == null && rawDoc.containsKey("high_price")) {
                    Object highPriceObj = rawDoc.get("high_price");
                    if (highPriceObj != null && !"N/A".equals(highPriceObj.toString())) {
                        stockData.setHighPrice(parseStringOrNumericPrice(highPriceObj));
                    }
                }
                
                if (stockData.getLowPrice() == null && rawDoc.containsKey("low_price")) {
                    Object lowPriceObj = rawDoc.get("low_price");
                    if (lowPriceObj != null && !"N/A".equals(lowPriceObj.toString())) {
                        stockData.setLowPrice(parseStringOrNumericPrice(lowPriceObj));
                    }
                }
                
                if (stockData.getPercentageChange() == null && rawDoc.containsKey("percentage_change")) {
                    Object percentageChangeObj = rawDoc.get("percentage_change");
                    if (percentageChangeObj != null && !"N/A".equals(percentageChangeObj.toString()) && !"0".equals(percentageChangeObj.toString())) {
                        stockData.setPercentageChange(parseStringOrNumericPrice(percentageChangeObj));
                    }
                }
                
                if (stockData.getPriceChange() == null && rawDoc.containsKey("price_change")) {
                    Object priceChangeObj = rawDoc.get("price_change");
                    if (priceChangeObj != null && !"N/A".equals(priceChangeObj.toString()) && !"0.00".equals(priceChangeObj.toString())) {
                        stockData.setPriceChange(parseStringOrNumericPrice(priceChangeObj));
                    }
                }
                
                logger.debug("Normalized data for {} on {}: close={}, open={}, high={}, low={}", 
                           stockData.getSymbol(), stockData.getDate(), 
                           stockData.getClosingPrice(), stockData.getOpenPrice(),
                           stockData.getHighPrice(), stockData.getLowPrice());
            }
            
        } catch (Exception e) {
            logger.debug("Error normalizing data for {} on {}: {}", 
                        stockData.getSymbol(), stockData.getDate(), e.getMessage());
        }
        
        return stockData;
    }
    
    /**
     * Helper method to parse price values from various formats
     */
    private BigDecimal parseStringOrNumericPrice(Object priceObj) {
        if (priceObj == null) return null;
        
        try {
            if (priceObj instanceof Number) {
                return BigDecimal.valueOf(((Number) priceObj).doubleValue()).setScale(2, RoundingMode.HALF_UP);
            } else if (priceObj instanceof String) {
                String priceStr = priceObj.toString().trim();
                if (priceStr.isEmpty() || "N/A".equals(priceStr) || "0".equals(priceStr)) {
                    return null;
                }
                return new BigDecimal(priceStr).setScale(2, RoundingMode.HALF_UP);
            }
        } catch (Exception e) {
            logger.debug("Failed to parse price: {}", priceObj);
        }
        
        return null;
    }
    
    /**
     * ENHANCED: Get stock data with normalization and percentage filter
     */
    public List<StockData> getStockDataWithPercentageFilter(String symbol, LocalDate fromDate, 
                                                           LocalDate toDate, Double minPercentage, 
                                                           Double maxPercentage) {
        try {
            List<StockData> rawData;
            
            if (minPercentage != null && maxPercentage != null) {
                rawData = stockDataRepository.findBySymbolAndDateBetweenAndPercentageChangeBetween(
                    symbol, fromDate, toDate, 
                    BigDecimal.valueOf(minPercentage), BigDecimal.valueOf(maxPercentage)
                );
            } else if (minPercentage != null) {
                rawData = stockDataRepository.findBySymbolAndDateBetweenAndPercentageChangeGreaterThanEqual(
                    symbol, fromDate, toDate, BigDecimal.valueOf(minPercentage)
                );
            } else if (maxPercentage != null) {
                rawData = stockDataRepository.findBySymbolAndDateBetweenAndPercentageChangeLessThanEqual(
                    symbol, fromDate, toDate, BigDecimal.valueOf(maxPercentage)
                );
            } else {
                rawData = stockDataRepository.findBySymbolAndDateBetween(symbol, fromDate, toDate);
            }
            
            // NORMALIZATION: Apply data normalization to all retrieved records
            List<StockData> normalizedData = rawData.stream()
                .map(this::normalizeStockData)
                .filter(stock -> stock.getClosingPrice() != null) // Only keep records with valid data
                .collect(Collectors.toList());
            
            logger.debug("Retrieved {} raw records, normalized to {} valid records for {} from {} to {}", 
                        rawData.size(), normalizedData.size(), symbol, fromDate, toDate);
            
            return normalizedData;
            
        } catch (Exception e) {
            logger.error("Error filtering stock data: {}", e.getMessage());
            return new ArrayList<>();
        }
    }
    
    /**
     * ENHANCED: Get stock data with MA filters and normalization
     */
    public List<StockData> getStockDataWithMAFilters(String symbol, LocalDate fromDate, 
                                                    LocalDate toDate, String maSignal100, 
                                                    String maSignal200, String goldenCross) {
        try {
            List<StockData> rawData = stockDataRepository.findBySymbolAndDateBetween(symbol, fromDate, toDate);
            
            // NORMALIZATION: Apply data normalization first
            List<StockData> normalizedData = rawData.stream()
                .map(this::normalizeStockData)
                .filter(stock -> stock.getClosingPrice() != null)
                .collect(Collectors.toList());
            
            return normalizedData.stream()
                .filter(stock -> {
                                   
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
     * ENHANCED: Get recent data for all stocks with normalization
     */
    public List<StockData> getRecentDataForAllStocks(LocalDate fromDate, LocalDate toDate, Integer limit) {
        try {
            List<StockData> rawData = stockDataRepository.findByDateBetween(fromDate, toDate);
            
            // NORMALIZATION: Apply data normalization
            List<StockData> normalizedData = rawData.stream()
                .map(this::normalizeStockData)
                .filter(stock -> stock.getClosingPrice() != null)
                .sorted((a, b) -> b.getDate().compareTo(a.getDate()))
                .collect(Collectors.toList());
            
            // Apply limit if specified
            if (limit != null && limit > 0 && normalizedData.size() > limit) {
                normalizedData = normalizedData.subList(0, limit);
            }
            
            logger.info("Retrieved {} raw records, normalized to {} valid records", 
                       rawData.size(), normalizedData.size());
            
            return normalizedData;
            
        } catch (Exception e) {
            logger.error("Error getting recent data for all stocks: {}", e.getMessage());
            return new ArrayList<>();
        }
    }
    
    /**
     * ENHANCED: Get normalized data for moving averages calculation
     */
    public List<StockData> getNormalizedDataForSymbol(String symbol) {
        try {
            List<StockData> rawData = stockDataRepository.findBySymbol(symbol);
            
            // NORMALIZATION: Critical for moving averages calculation
            List<StockData> normalizedData = rawData.stream()
                .map(this::normalizeStockData)
                .filter(stock -> stock.getClosingPrice() != null)
                .sorted(Comparator.comparing(StockData::getDate))
                .collect(Collectors.toList());
            
            logger.debug("Symbol {}: {} raw records -> {} normalized records with valid prices", 
                        symbol, rawData.size(), normalizedData.size());
            
            return normalizedData;
            
        } catch (Exception e) {
            logger.error("Error getting normalized data for {}: {}", symbol, e.getMessage());
            return new ArrayList<>();
        }
    }
    
        /**
     * Daily cron job - fetches only today's data since historical data already exists
     * Runs at 7:00 PM IST on weekdays, automatically skips weekends and holidays
     */
    @Scheduled(cron = "0 0 19 * * MON-FRI", zone = "Asia/Kolkata")
    public void scheduledDailyDataFetch() {
        if (!schedulerEnabled) {
            logger.info("Scheduler disabled, skipping daily data fetch");
            return;
        }
        
        LocalDate today = LocalDate.now();
        logger.info("Cron job triggered at {} for date: {}", LocalDateTime.now(), today);
        
        try {
            // Check if market is open (skip holidays)
            if (INDIAN_MARKET_HOLIDAYS.contains(today.toString())) {
                logger.info("Market holiday detected ({}), skipping data fetch", today);
                return;
            }
            
            // Check if weekend (this shouldn't happen due to cron schedule, but safety check)
            if (today.getDayOfWeek().getValue() >= 6) {
                logger.info("Weekend detected ({}), skipping data fetch", today);
                return;
            }
            
            logger.info("Market is open, fetching today's data for: {}", today);
            
            // Fetch today's stock data
            int recordsProcessed = fetchTodaysStockData(today);
            
            // Calculate moving averages if data was processed
            if (movingAveragesEnabled && recordsProcessed > 0) {
                logger.info("Calculating moving averages for recent data...");
                movingAverageService.calculateMovingAveragesForRecentData(today.minusDays(1));
            }
            
            // Record successful job completion
            String message = String.format("Successfully processed %d records for %s. Moving averages: %s", 
                                        recordsProcessed, today, movingAveragesEnabled ? "CALCULATED" : "DISABLED");
            jobExecutionService.recordJobCompletion("DAILY_STOCK_FETCH", "SUCCESS", message);
            
            logger.info("Daily job completed successfully: {} records processed", recordsProcessed);
            
        } catch (Exception e) {
            logger.error("Daily job failed: {}", e.getMessage(), e);
            jobExecutionService.recordJobError("DAILY_STOCK_FETCH", e);
        }
    }
      
    private StockData parseKiteApiData(String symbol, JsonNode dayData) {
        try {
            if (!dayData.isArray()) {
                logger.warn("Expected array format for {}, got: {}", symbol, dayData.getNodeType());
                return null;
            }
            
            if (dayData.size() < 6) {
                logger.warn("Invalid data array size for {}: expected 6 elements, got {}", 
                           symbol, dayData.size());
                return null;
            }
            
            String timestamp = dayData.get(0).asText();
            LocalDate date;
            
            try {
                if (timestamp.contains("T")) {
                    date = LocalDate.parse(timestamp.substring(0, 10));
                } else if (timestamp.contains("-") && timestamp.length() >= 10) {
                    date = LocalDate.parse(timestamp.substring(0, 10));
                } else {
                    logger.warn("Unrecognized timestamp format for {}: {}", symbol, timestamp);
                    return null;
                }
            } catch (Exception e) {
                logger.error("Failed to parse date for {}: {} - {}", symbol, timestamp, e.getMessage());
                return null;
            }
            
            BigDecimal openPrice, highPrice, lowPrice, closingPrice;
            Long volume;
            
            try {
                openPrice = parsePrice(dayData.get(1), "open", symbol);
                highPrice = parsePrice(dayData.get(2), "high", symbol);
                lowPrice = parsePrice(dayData.get(3), "low", symbol);
                closingPrice = parsePrice(dayData.get(4), "close", symbol);
                
                if (openPrice == null || highPrice == null || lowPrice == null || closingPrice == null) {
                    logger.warn("Invalid price data for {} on {}", symbol, date);
                    return null;
                }
                
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
            
            if (!isValidOHLCData(openPrice, highPrice, lowPrice, closingPrice)) {
                logger.warn("Invalid OHLC relationships for {} on {}: O={}, H={}, L={}, C={}", 
                           symbol, date, openPrice, highPrice, lowPrice, closingPrice);
                return null;
            }
            
            return new StockData(symbol, date, openPrice, highPrice, lowPrice, closingPrice, volume, BigDecimal.ZERO);
            
        } catch (Exception e) {
            logger.error("Unexpected error parsing Kite data for {}: {}", symbol, e.getMessage());
            return null;
        }
    }
    
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
    
    private boolean isValidOHLCData(BigDecimal open, BigDecimal high, BigDecimal low, BigDecimal close) {
        if (open == null || high == null || low == null || close == null) {
            return false;
        }
        
        if (high.compareTo(open) < 0 || high.compareTo(close) < 0 || high.compareTo(low) < 0) {
            return false;
        }
        
        if (low.compareTo(open) > 0 || low.compareTo(close) > 0 || low.compareTo(high) > 0) {
            return false;
        }
        
        return true;
    }
    
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
    
    public Set<String> getAvailableStockSymbols() {
        return NIFTY50_INSTRUMENTS.keySet();
    }
    
    public Map<String, Object> getSystemStats() {
        Map<String, Object> stats = new HashMap<>();
        
        try {
            long totalRecords = stockDataRepository.count();
            List<String> distinctSymbols = stockDataRepository.findDistinctSymbols();
            
            Optional<StockData> latestRecord = stockDataRepository.findAll()
                .stream()
                .max(Comparator.comparing(StockData::getDate));
            
            Optional<StockData> earliestRecord = stockDataRepository.findAll()
                .stream()
                .min(Comparator.comparing(StockData::getDate));
            
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
            
            if (kiteAuthService.isAuthenticated()) {
                stats.put("apiConnectionHealthy", kiteApiService.testConnection());
            } else {
                stats.put("apiConnectionHealthy", false);
            }
            
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
    
    
    private boolean isWeekend(LocalDate date) {
        return date.getDayOfWeek().getValue() >= 6;
    }
    
    public String getInstrumentToken(String symbol) {
        return NIFTY50_INSTRUMENTS.get(symbol.toUpperCase());
    }
    
    public boolean isValidNifty50Symbol(String symbol) {
        return NIFTY50_INSTRUMENTS.containsKey(symbol.toUpperCase());
    }
    
    public long getTotalDataPointsCount() {
        return stockDataRepository.count();
    }
    
    public List<StockData> getStockDataAfterDate(LocalDate fromDate) {
        return stockDataRepository.findByDateGreaterThanEqual(fromDate);
    }
    
    public List<StockData> getStockDataBySymbolAndDateAfter(String symbol, LocalDate fromDate) {
        return stockDataRepository.findBySymbolAndDateGreaterThanEqual(symbol, fromDate);
    }
    
    public List<StockData> getStockDataWithMovingAveragesAfterDate(LocalDate fromDate) {
        return stockDataRepository.findByDateGreaterThanEqualAndMovingAverage100DayIsNotNull(fromDate);
    }
    
    public List<StockData> getFilteredStockData(String symbol, LocalDate fromDate, LocalDate toDate, 
                                            BigDecimal minPercentage, BigDecimal maxPercentage, 
                                            Long minVolume, String sortBy, Integer limit) {
        
        List<StockData> result = stockDataRepository.findByDateBetween(fromDate, toDate);
        
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
        
        switch (sortBy) {
            case "percentageChange":
                result.sort((a, b) -> {
                    BigDecimal aChange = a.getPercentageChange() != null ? a.getPercentageChange() : BigDecimal.ZERO;
                    BigDecimal bChange = b.getPercentageChange() != null ? b.getPercentageChange() : BigDecimal.ZERO;
                    return bChange.compareTo(aChange);
                });
                break;
            case "volume":
                result.sort((a, b) -> {
                    Long aVolume = a.getVolume() != null ? a.getVolume() : 0L;
                    Long bVolume = b.getVolume() != null ? b.getVolume() : 0L;
                    return bVolume.compareTo(aVolume);
                });
                break;
            case "symbol":
                result.sort((a, b) -> a.getSymbol().compareTo(b.getSymbol()));
                break;
            case "date":
            default:
                result.sort((a, b) -> b.getDate().compareTo(a.getDate()));
                break;
        }
        
        if (limit != null && limit > 0 && result.size() > limit) {
            result = result.subList(0, limit);
        }
        
        return result;
    }
    
    public List<StockData> getStockDataWithPercentageChangeGreaterThan(BigDecimal threshold, LocalDate fromDate) {
        return stockDataRepository.findByDateGreaterThanEqualAndPercentageChangeGreaterThanEqual(fromDate, threshold);
    }
    
    public List<StockData> getStockDataWithPercentageChangeLessThan(BigDecimal threshold, LocalDate fromDate) {
        return stockDataRepository.findByDateGreaterThanEqualAndPercentageChangeLessThanEqual(fromDate, threshold);
    }
    
    public List<StockData> getStockDataWithAbsolutePercentageChangeGreaterThan(BigDecimal threshold, LocalDate fromDate) {
        List<StockData> positiveChanges = getStockDataWithPercentageChangeGreaterThan(threshold, fromDate);
        List<StockData> negativeChanges = getStockDataWithPercentageChangeLessThan(threshold.negate(), fromDate);
        
        List<StockData> result = new ArrayList<>(positiveChanges);
        result.addAll(negativeChanges);
        
        return result.stream()
            .sorted((a, b) -> b.getDate().compareTo(a.getDate()))
            .collect(Collectors.toList());
    }
    
    public long getDaysAnalyzedCount() {
        try {
            Long count = stockDataRepository.countDistinctDates();
            return count != null ? count : 0L;
        } catch (Exception e) {
            logger.warn("Could not get distinct dates count, using fallback calculation: {}", e.getMessage());
            long totalRecords = stockDataRepository.count();
            int numberOfStocks = NIFTY50_INSTRUMENTS.size();
            return numberOfStocks > 0 ? Math.max(1, totalRecords / numberOfStocks) : 0L;
        }
    }
    
    public boolean isKiteAuthenticated() {
        return kiteAuthService.isAuthenticated();
    }

        /**
     * Fetch today's stock data for all Nifty 50 stocks
     * Simple implementation since historical data already exists
     */
    private int fetchTodaysStockData(LocalDate date) {
        String jobName = "DAILY_STOCK_FETCH";
        jobExecutionService.recordJobStart(jobName);
        
        int recordsProcessed = 0;
        int successfulStocks = 0;
        int failedStocks = 0;
        List<String> failedSymbols = new ArrayList<>();
        
        try {
            if (!kiteAuthService.isAuthenticated()) {
                logger.error("Kite API not authenticated. Cannot fetch real data.");
                return 0;
            }
            
            boolean connectionOk = kiteApiService.testConnection();
            if (!connectionOk) {
                logger.error("Kite API connection test failed. Check authentication.");
                return 0;
            }
            
            logger.info("Fetching data for {} Nifty 50 stocks for date: {}", NIFTY50_INSTRUMENTS.size(), date);
            
            for (Map.Entry<String, String> entry : NIFTY50_INSTRUMENTS.entrySet()) {
                String symbol = entry.getKey();
                String instrumentToken = entry.getValue();
                
                try {
                    logger.debug("Processing {}: {}", symbol, instrumentToken);
                    
                    // Fetch today's data only (not historical range)
                    int recordsForSymbol = fetchAndProcessDailyData(symbol, instrumentToken, date);
                    
                    if (recordsForSymbol > 0) {
                        recordsProcessed += recordsForSymbol;
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
            
            logger.info("Data fetch completed: {} successful, {} failed. Total records: {}", 
                    successfulStocks, failedStocks, recordsProcessed);
            
            if (!failedSymbols.isEmpty()) {
                logger.warn("Failed symbols: {}", String.join(", ", failedSymbols));
            }
            
            return recordsProcessed;
            
        } catch (Exception e) {
            logger.error("Fatal error in daily data fetch: {}", e.getMessage(), e);
            return 0;
        }
    }

    /**
     * Fetch and process data for a single stock for today's date
     */
    private int fetchAndProcessDailyData(String symbol, String instrumentToken, LocalDate date) {
        try {
            if (!kiteApiService.isValidInstrumentToken(instrumentToken)) {
                logger.error("Invalid instrument token for {}: {}", symbol, instrumentToken);
                return 0;
            }
            
            // Fetch today's data (single day)
            JsonNode historicalData = kiteApiService.fetchHistoricalData(instrumentToken, date, date);
            
            if (historicalData == null || !historicalData.isArray() || historicalData.size() == 0) {
                logger.warn("No data received for {} on {}", symbol, date);
                return 0;
            }
            
            // Process the single day's data
            JsonNode dayData = historicalData.get(0);
            StockData stockData = parseKiteApiData(symbol, dayData);
            
            if (stockData != null) {
                // Calculate percentage change vs previous day
                Optional<StockData> previousData = stockDataRepository
                    .findLatestBySymbol(symbol);
                
                if (previousData.isPresent()) {
                    BigDecimal percentageChange = calculatePercentageChange(
                        previousData.get().getClosingPrice(), 
                        stockData.getClosingPrice()
                    );
                    stockData.setPercentageChange(percentageChange);
                } else {
                    stockData.setPercentageChange(BigDecimal.ZERO);
                }
                
                // Save or update the record
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
                
                logger.debug("Saved data for {} on {}: close={}, change={}%", 
                            symbol, date, stockData.getClosingPrice(), stockData.getPercentageChange());
                
                return 1;
            }
            
            return 0;
            
        } catch (Exception e) {
            logger.error("Error processing daily data for {} on {}: {}", symbol, date, e.getMessage());
            return 0;
        }
    }

    /**
     * Check if a date is a market holiday
     */
    public boolean isMarketHoliday(LocalDate date) {
        return INDIAN_MARKET_HOLIDAYS.contains(date.toString()) || 
            date.getDayOfWeek().getValue() >= 6;
    }

    /**
     * Get next market working day
     */
    public LocalDate getNextMarketDay(LocalDate fromDate) {
        LocalDate nextDay = fromDate.plusDays(1);
        
        while (isMarketHoliday(nextDay)) {
            nextDay = nextDay.plusDays(1);
            if (nextDay.isAfter(fromDate.plusDays(10))) {
                logger.warn("Could not find next market day within 10 days of {}", fromDate);
                break;
            }
        }
        
        return nextDay;
    }

}