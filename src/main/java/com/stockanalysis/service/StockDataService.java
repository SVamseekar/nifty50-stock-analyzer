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
// REMOVED: import java.time.format.DateTimeFormatter; (was unused)
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Enhanced Stock Data Service with Moving Average support
 * Now calculates 100-day and 200-day moving averages
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
    private MovingAverageService movingAverageService;  // NEW
    
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
    
    // Nifty 50 stocks with their Kite instrument tokens (NSE)
    private static final Map<String, String> NIFTY50_INSTRUMENTS = new HashMap<String, String>() {{
        put("RELIANCE", "738561");     // Reliance Industries
        put("TCS", "2953217");         // Tata Consultancy Services
        put("HDFCBANK", "341249");     // HDFC Bank
        put("INFY", "408065");         // Infosys
        put("ICICIBANK", "1270529");   // ICICI Bank
        put("HINDUNILVR", "356865");   // Hindustan Unilever
        put("ITC", "424961");          // ITC
        put("SBIN", "779521");         // State Bank of India
        put("BHARTIARTL", "2714625");  // Bharti Airtel
        put("KOTAKBANK", "492033");    // Kotak Mahindra Bank
        put("ASIANPAINT", "60417");    // Asian Paints
        put("LT", "2939649");          // Larsen & Toubro
        put("AXISBANK", "1510401");    // Axis Bank
        put("MARUTI", "2815745");      // Maruti Suzuki
        put("SUNPHARMA", "857857");    // Sun Pharmaceutical
        put("TITAN", "897537");        // Titan Company
        put("ULTRACEMCO", "2952193");  // UltraTech Cement
        put("NESTLEIND", "4598529");   // Nestle India
        put("WIPRO", "969473");        // Wipro
        put("HCLTECH", "1850625");     // HCL Technologies
        put("BAJFINANCE", "81153");    // Bajaj Finance
        put("TECHM", "3465729");       // Tech Mahindra
        put("NTPC", "2977281");        // NTPC
        put("POWERGRID", "3834113");   // Power Grid Corporation
        put("TATAMOTORS", "884737");   // Tata Motors
        put("COALINDIA", "5215745");   // Coal India
        put("TATASTEEL", "895745");    // Tata Steel
        put("BAJAJFINSV", "4268801");  // Bajaj Finserv
        put("HDFCLIFE", "119553");     // HDFC Life Insurance
        put("ONGC", "633601");         // Oil & Natural Gas Corporation
        put("M&M", "519937");          // Mahindra & Mahindra
        put("SBILIFE", "5582849");     // SBI Life Insurance
        put("JSWSTEEL", "3001089");    // JSW Steel
        put("BRITANNIA", "140033");    // Britannia Industries
        put("GRASIM", "315393");       // Grasim Industries
        put("DRREDDY", "225537");      // Dr. Reddy's Laboratories
        put("CIPLA", "177665");        // Cipla
        put("EICHERMOT", "232961");    // Eicher Motors
        put("ADANIENT", "6401");       // Adani Enterprises
        put("APOLLOHOSP", "41729");    // Apollo Hospitals Enterprise
        put("DIVISLAB", "2800641");    // Divi's Laboratories
        put("INDUSINDBK", "1346049");  // IndusInd Bank
        put("HINDALCO", "348929");     // Hindalco Industries
        put("HEROMOTOCO", "345089");   // Hero MotoCorp
        put("TATACONSUM", "878593");   // Tata Consumer Products
        put("BPCL", "134657");         // Bharat Petroleum Corporation
        put("LTIM", "11483906");       // LTIMindtree
        put("ADANIPORTS", "3861249");  // Adani Ports and SEZ
        put("UPL", "2889473");         // UPL Limited
        put("BAJAJ-AUTO", "78081");    // Bajaj Auto
    }};
    
    /**
     * ENHANCED: Scheduled job to fetch daily data AND calculate moving averages
     * Runs every day at 7 PM IST (after market close)
     */
    @Scheduled(cron = "0 0 19 * * MON-FRI", zone = "Asia/Kolkata")
    public void scheduledDataFetch() {
        if (!schedulerEnabled) {
            logger.info("Scheduler disabled, skipping scheduled data fetch");
            return;
        }
        
        logger.info("🚀 Starting scheduled daily data fetch with moving averages at {}", LocalDate.now());
        
        try {
            // Get last successful run date
            Optional<LocalDate> lastRunDate = jobExecutionService.getLastSuccessfulRunDate("DAILY_STOCK_FETCH");
            
            LocalDate fromDate = lastRunDate.orElse(LocalDate.now().minusDays(5)); // Default to 5 days back
            LocalDate toDate = LocalDate.now();
            
            logger.info("📊 Fetching data from {} to {}", fromDate, toDate);
            
            // Step 1: Fetch new stock data
            int recordsProcessed = fetchAllHistoricalData(fromDate, toDate);
            
            // Step 2: Calculate moving averages for updated data
            if (movingAveragesEnabled && recordsProcessed > 0) {
                logger.info("🧮 Calculating moving averages for recent data...");
                movingAverageService.calculateMovingAveragesForRecentData(fromDate);
            }
            
            logger.info("✅ Scheduled job completed: {} records processed, moving averages updated", recordsProcessed);
            
        } catch (Exception e) {
            logger.error("❌ Scheduled job failed: {}", e.getMessage(), e);
            jobExecutionService.recordJobError("DAILY_STOCK_FETCH", e);
        }
    }
    
    /**
     * ENHANCED: Fetch historical data AND calculate moving averages
     */
    public int fetchAllHistoricalData(LocalDate fromDate, LocalDate toDate) {
        String jobName = "DAILY_STOCK_FETCH_WITH_MA";
        jobExecutionService.recordJobStart(jobName);
        
        int totalRecordsProcessed = 0;
        int successfulStocks = 0;
        int failedStocks = 0;
        
        try {
            logger.info("🚀 Starting Nifty 50 data fetch for {} stocks from {} to {}", 
                       NIFTY50_INSTRUMENTS.size(), fromDate, toDate);
            
            if (!useRealData || !kiteAuthService.isAuthenticated()) {
                logger.warn("⚠️ Real data disabled or not authenticated. Using mock data.");
                totalRecordsProcessed = generateMockData(fromDate, toDate);
            } else {
                // Fetch real data from Kite API
                for (Map.Entry<String, String> entry : NIFTY50_INSTRUMENTS.entrySet()) {
                    String symbol = entry.getKey();
                    String instrumentToken = entry.getValue();
                    
                    try {
                        logger.debug("📊 Processing {}: {}", symbol, instrumentToken);
                        
                        int recordsForSymbol = fetchAndProcessHistoricalData(symbol, instrumentToken, fromDate, toDate);
                        totalRecordsProcessed += recordsForSymbol;
                        successfulStocks++;
                        
                        logger.debug("✅ {} - {} records processed", symbol, recordsForSymbol);
                        
                        // Rate limiting - respect Kite API limits
                        kiteApiService.rateLimit();
                        
                    } catch (Exception e) {
                        logger.error("❌ Failed to process {}: {}", symbol, e.getMessage());
                        failedStocks++;
                        // Continue with next stock instead of failing entire job
                    }
                }
            }
            
            // STEP 2: Calculate moving averages if enabled
            if (movingAveragesEnabled && totalRecordsProcessed > 0) {
                logger.info("🧮 Calculating moving averages for all updated stocks...");
                try {
                    movingAverageService.calculateMovingAveragesForAllStocks();
                    logger.info("✅ Moving averages calculation completed");
                } catch (Exception e) {
                    logger.error("❌ Moving averages calculation failed: {}", e.getMessage());
                    // Don't fail the entire job for MA calculation errors
                }
            }
            
            String message = String.format("Successfully processed %d stocks, %d failed. Total records: %d. Moving averages: %s", 
                                         successfulStocks, failedStocks, totalRecordsProcessed,
                                         movingAveragesEnabled ? "CALCULATED" : "DISABLED");
            
            jobExecutionService.recordJobCompletion(jobName, "SUCCESS", message);
            logger.info("🎉 Data fetch completed: {}", message);
            
            return totalRecordsProcessed;
            
        } catch (Exception e) {
            logger.error("💥 Fatal error in data fetch job: {}", e.getMessage(), e);
            jobExecutionService.recordJobError(jobName, e);
            throw new RuntimeException("Data fetch job failed", e);
        }
    }
    
    /**
     * ENHANCED: Manual trigger for moving averages calculation
     */
    public Map<String, Object> triggerMovingAveragesCalculation() {
        Map<String, Object> result = new HashMap<>();
        
        try {
            logger.info("🚀 Manual trigger: Calculating moving averages for all stocks");
            
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
            
            logger.info("✅ Moving averages calculation completed in {}ms", duration);
            
        } catch (Exception e) {
            logger.error("❌ Moving averages calculation failed: {}", e.getMessage(), e);
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
            // Start with basic date filter
            List<StockData> data = stockDataRepository.findBySymbolAndDateBetween(symbol, fromDate, toDate);
            
            // Apply moving average filters
            return data.stream()
                .filter(stock -> {
                    // Filter by 100-day MA signal
                    if (maSignal100 != null && !maSignal100.equals("ALL")) {
                        if (!maSignal100.equals(stock.getMaSignal100())) {
                            return false;
                        }
                    }
                    
                    // Filter by 200-day MA signal
                    if (maSignal200 != null && !maSignal200.equals("ALL")) {
                        if (!maSignal200.equals(stock.getMaSignal200())) {
                            return false;
                        }
                    }
                    
                    // Filter by golden cross signal
                    if (goldenCross != null && !goldenCross.equals("ALL")) {
                        if (!goldenCross.equals(stock.getGoldenCross())) {
                            return false;
                        }
                    }
                    
                    return true;
                })
                .collect(Collectors.toList());
                
        } catch (Exception e) {
            logger.error("❌ Error filtering stock data with MA filters: {}", e.getMessage());
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
            logger.error("❌ Error finding golden cross stocks: {}", e.getMessage());
            result.put("status", "ERROR");
            result.put("error", e.getMessage());
        }
        
        return result;
    }
    
    // Keep all other methods unchanged... (they are complete and working)
    
    /**
     * Fetch and process historical data for a single stock
     */
    private int fetchAndProcessHistoricalData(String symbol, String instrumentToken, 
                                            LocalDate fromDate, LocalDate toDate) {
        try {
            // Fetch data from Kite API
            JsonNode historicalData = kiteApiService.fetchHistoricalData(instrumentToken, fromDate, toDate);
            
            if (historicalData == null || !historicalData.isArray()) {
                logger.warn("⚠️ No data received for {}", symbol);
                return 0;
            }
            
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
                    logger.warn("⚠️ Failed to parse day data for {}: {}", symbol, e.getMessage());
                }
            }
            
            // Save all records in batch
            if (!stockDataList.isEmpty()) {
                stockDataRepository.saveAll(stockDataList);
                logger.debug("💾 Saved {} records for {}", stockDataList.size(), symbol);
            }
            
            return recordsProcessed;
            
        } catch (Exception e) {
            logger.error("❌ Error processing historical data for {}: {}", symbol, e.getMessage());
            throw new RuntimeException("Failed to process " + symbol, e);
        }
    }
    
    /**
     * Parse Kite API response data into StockData object
     */
    private StockData parseKiteApiData(String symbol, JsonNode dayData) {
        try {
            // Kite API returns array: [date, open, high, low, close, volume, oi]
            if (!dayData.isArray() || dayData.size() < 6) {
                logger.warn("⚠️ Invalid data format for {}: {}", symbol, dayData);
                return null;
            }
            
            // Parse date (first element)
            String dateStr = dayData.get(0).asText();
            LocalDate date = LocalDate.parse(dateStr.substring(0, 10)); // Extract YYYY-MM-DD
            
            // Parse OHLCV data
            BigDecimal openPrice = new BigDecimal(dayData.get(1).asDouble()).setScale(2, RoundingMode.HALF_UP);
            BigDecimal highPrice = new BigDecimal(dayData.get(2).asDouble()).setScale(2, RoundingMode.HALF_UP);
            BigDecimal lowPrice = new BigDecimal(dayData.get(3).asDouble()).setScale(2, RoundingMode.HALF_UP);
            BigDecimal closingPrice = new BigDecimal(dayData.get(4).asDouble()).setScale(2, RoundingMode.HALF_UP);
            Long volume = dayData.get(5).asLong();
            
            // Create and return StockData object
            return new StockData(symbol, date, openPrice, highPrice, lowPrice, closingPrice, volume, BigDecimal.ZERO);
            
        } catch (Exception e) {
            logger.error("❌ Error parsing Kite data for {}: {}", symbol, e.getMessage());
            return null;
        }
    }
    
    /**
     * Calculate percentage change between two prices
     */
    private BigDecimal calculatePercentageChange(BigDecimal previousPrice, BigDecimal currentPrice) {
        if (previousPrice == null || currentPrice == null || previousPrice.compareTo(BigDecimal.ZERO) == 0) {
            return BigDecimal.ZERO;
        }
        
        BigDecimal change = currentPrice.subtract(previousPrice);
        BigDecimal percentage = change.divide(previousPrice, 4, RoundingMode.HALF_UP)
                                     .multiply(new BigDecimal("100"))
                                     .setScale(2, RoundingMode.HALF_UP);
        
        return percentage;
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
            logger.error("❌ Error filtering stock data: {}", e.getMessage());
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
     * ENHANCED: Get system statistics with moving averages info
     */
    public Map<String, Object> getSystemStats() {
        Map<String, Object> stats = new HashMap<>();
        
        try {
            // Basic counts
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
                // This could be optimized with aggregation queries
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
            logger.error("❌ Error getting system stats: {}", e.getMessage());
            stats.put("error", "Failed to get stats: " + e.getMessage());
        }
        
        return stats;
    }
    
    /**
     * Generate mock data when real API is not available
     */
    private int generateMockData(LocalDate fromDate, LocalDate toDate) {
        if (!mockDataEnabled) {
            logger.info("Mock data disabled, skipping data generation");
            return 0;
        }
        
        logger.info("🔄 Generating mock data for {} stocks from {} to {}", 
                   NIFTY50_INSTRUMENTS.size(), fromDate, toDate);
        
        Random random = new Random();
        int totalRecords = 0;
        
        for (String symbol : NIFTY50_INSTRUMENTS.keySet()) {
            BigDecimal basePrice = BigDecimal.valueOf(1000 + random.nextInt(4000)); // Random base price
            LocalDate currentDate = fromDate;
            
            while (!currentDate.isAfter(toDate)) {
                // Skip weekends
                if (isWeekend(currentDate)) {
                    currentDate = currentDate.plusDays(1);
                    continue;
                }
                
                // Generate realistic OHLCV data
                BigDecimal volatility = BigDecimal.valueOf(0.02 + random.nextGaussian() * 0.01); // 2% avg volatility
                BigDecimal change = basePrice.multiply(volatility);
                
                BigDecimal open = basePrice.add(change.multiply(BigDecimal.valueOf(random.nextGaussian() * 0.5)));
                BigDecimal close = open.add(change.multiply(BigDecimal.valueOf(random.nextGaussian())));
                BigDecimal high = close.max(open).add(change.multiply(BigDecimal.valueOf(Math.abs(random.nextGaussian() * 0.3))));
                BigDecimal low = close.min(open).subtract(change.multiply(BigDecimal.valueOf(Math.abs(random.nextGaussian() * 0.3))));
                
                Long volume = 100000L + random.nextInt(900000); // Random volume
                BigDecimal percentageChange = calculatePercentageChange(basePrice, close);
                
                StockData stockData = new StockData(symbol, currentDate, 
                    open.setScale(2, RoundingMode.HALF_UP),
                    high.setScale(2, RoundingMode.HALF_UP),
                    low.setScale(2, RoundingMode.HALF_UP),
                    close.setScale(2, RoundingMode.HALF_UP),
                    volume, percentageChange);
                
                try {
                    stockDataRepository.save(stockData);
                    totalRecords++;
                    basePrice = close; // Use closing price as next day's base
                } catch (Exception e) {
                    logger.debug("📝 Duplicate data for {} on {}, skipping", symbol, currentDate);
                }
                
                currentDate = currentDate.plusDays(1);
            }
        }
        
        logger.info("✅ Generated {} mock records", totalRecords);
        return totalRecords;
    }
    
    /**
     * Check if date is weekend
     */
    private boolean isWeekend(LocalDate date) {
        return date.getDayOfWeek().getValue() >= 6; // Saturday = 6, Sunday = 7
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
}