package com.stockanalysis.repository;

import com.stockanalysis.model.StockData;
import org.springframework.data.mongodb.repository.Aggregation;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.stereotype.Repository;
import java.time.LocalDate;
import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

@Repository
public interface StockDataRepository extends MongoRepository<StockData, String> {
    
    // ==========================================
    // BASIC QUERIES
    // ==========================================
    
    /**
     * Find stock data by symbol and date range
     */
    List<StockData> findBySymbolAndDateBetween(String symbol, LocalDate startDate, LocalDate endDate);
    
    /**
     * Find all data for a specific symbol
     */
    List<StockData> findBySymbol(String symbol);
    
    /**
     * Find specific stock data by symbol and date
     */
    Optional<StockData> findBySymbolAndDate(String symbol, LocalDate date);
    
    /**
     * Get latest data for a symbol (last N records)
     */
    List<StockData> findTop10BySymbolOrderByDateDesc(String symbol);
    
    // ==========================================
    // PERCENTAGE CHANGE QUERIES
    // ==========================================
    
    /**
     * Find stocks with percentage change between min and max
     */
    @Query("{'symbol': ?0, 'date': {$gte: ?1, $lte: ?2}, 'percentageChange': {$gte: ?3, $lte: ?4}}")
    List<StockData> findBySymbolAndDateBetweenAndPercentageChangeBetween(
        String symbol, LocalDate startDate, LocalDate endDate, 
        BigDecimal minPercentage, BigDecimal maxPercentage);
    
    /**
     * Find stocks with percentage change greater than or equal to threshold
     */
    @Query("{'symbol': ?0, 'date': {$gte: ?1, $lte: ?2}, 'percentageChange': {$gte: ?3}}")
    List<StockData> findBySymbolAndDateBetweenAndPercentageChangeGreaterThanEqual(
        String symbol, LocalDate startDate, LocalDate endDate, BigDecimal minPercentage);
    
    /**
     * Find stocks with percentage change less than or equal to threshold
     */
    @Query("{'symbol': ?0, 'date': {$gte: ?1, $lte: ?2}, 'percentageChange': {$lte: ?3}}")
    List<StockData> findBySymbolAndDateBetweenAndPercentageChangeLessThanEqual(
        String symbol, LocalDate startDate, LocalDate endDate, BigDecimal maxPercentage);
    
    /**
     * Find positive changes only
     */
    @Query("{'symbol': ?0, 'date': {$gte: ?1, $lte: ?2}, 'percentageChange': {$gt: 0}}")
    List<StockData> findPositiveChanges(String symbol, LocalDate startDate, LocalDate endDate);
    
    /**
     * Find negative changes only
     */
    @Query("{'symbol': ?0, 'date': {$gte: ?1, $lte: ?2}, 'percentageChange': {$lt: 0}}")
    List<StockData> findNegativeChanges(String symbol, LocalDate startDate, LocalDate endDate);
    
    // ==========================================
    // VOLUME-BASED QUERIES
    // ==========================================
    
    /**
     * Find high volume trading days
     */
    @Query("{'symbol': ?0, 'date': {$gte: ?1, $lte: ?2}, 'volume': {$gte: ?3}}")
    List<StockData> findBySymbolAndDateBetweenAndVolumeGreaterThanEqual(
        String symbol, LocalDate startDate, LocalDate endDate, Long minVolume);
    
    // ==========================================
    // AGGREGATION QUERIES
    // ==========================================
    
    /**
     * Get distinct symbols using aggregation
     */
    @Aggregation(pipeline = {
        "{ '$group': { '_id': '$symbol' } }",
        "{ '$project': { '_id': 0, 'symbol': '$_id' } }"
    })
    List<String> findDistinctSymbols();
    
    /**
     * Get symbols with their record counts
     */
    @Aggregation(pipeline = {
        "{ '$group': { '_id': '$symbol', 'count': { '$sum': 1 } } }",
        "{ '$project': { '_id': 0, 'symbol': '$_id', 'count': 1 } }",
        "{ '$sort': { 'count': -1 } }"
    })
    List<Object> getSymbolCounts();
    
    /**
     * Get average percentage change by symbol
     */
    @Aggregation(pipeline = {
        "{ '$match': { 'symbol': ?0, 'date': { '$gte': ?1, '$lte': ?2 } } }",
        "{ '$group': { '_id': '$symbol', 'avgChange': { '$avg': '$percentageChange' }, 'count': { '$sum': 1 } } }"
    })
    List<Object> getAveragePercentageChange(String symbol, LocalDate startDate, LocalDate endDate);
    
    /**
     * Find top movers (highest percentage changes) across all symbols
     */
    @Aggregation(pipeline = {
        "{ '$match': { 'date': { '$gte': ?0, '$lte': ?1 } } }",
        "{ '$sort': { 'percentageChange': -1 } }",
        "{ '$limit': ?2 }"
    })
    List<StockData> findTopMovers(LocalDate startDate, LocalDate endDate, int limit);
    
    /**
     * Find bottom movers (lowest percentage changes) across all symbols
     */
    @Aggregation(pipeline = {
        "{ '$match': { 'date': { '$gte': ?0, '$lte': ?1 } } }",
        "{ '$sort': { 'percentageChange': 1 } }",
        "{ '$limit': ?2 }"
    })
    List<StockData> findBottomMovers(LocalDate startDate, LocalDate endDate, int limit);
    
    /**
     * Get daily statistics for a date range
     */
    @Aggregation(pipeline = {
        "{ '$match': { 'date': { '$gte': ?0, '$lte': ?1 } } }",
        "{ '$group': { '_id': '$date', 'avgChange': { '$avg': '$percentageChange' }, 'maxChange': { '$max': '$percentageChange' }, 'minChange': { '$min': '$percentageChange' }, 'totalVolume': { '$sum': '$volume' }, 'count': { '$sum': 1 } } }",
        "{ '$sort': { '_id': 1 } }"
    })
    List<Object> getDailyStatistics(LocalDate startDate, LocalDate endDate);
    
    // ==========================================
    // COUNT QUERIES
    // ==========================================
    
    /**
     * Count records for a symbol
     */
    long countBySymbol(String symbol);
    
    /**
     * Count records for symbol in date range
     */
    long countBySymbolAndDateBetween(String symbol, LocalDate startDate, LocalDate endDate);
    
    /**
     * Count records with significant changes (above threshold)
     */
    @Query(value = "{'symbol': ?0, 'date': {$gte: ?1, $lte: ?2}, $or: [{'percentageChange': {$gte: ?3}}, {'percentageChange': {$lte: ?4}}]}", count = true)
    long countSignificantChanges(String symbol, LocalDate startDate, LocalDate endDate, 
                                BigDecimal positiveThreshold, BigDecimal negativeThreshold);
    
    // ==========================================
    // DATE-BASED QUERIES
    // ==========================================
    
    /**
     * Find latest date for a symbol (most recent data)
     */
    @Query(value = "{'symbol': ?0}", sort = "{'date': -1}")
    Optional<StockData> findLatestBySymbol(String symbol);
    
    /**
     * Find earliest date for a symbol (oldest data)
     */
    @Query(value = "{'symbol': ?0}", sort = "{'date': 1}")
    Optional<StockData> findEarliestBySymbol(String symbol);
    
    /**
     * Find all records for a specific date across all symbols
     */
    List<StockData> findByDate(LocalDate date);
    
    /**
     * Check if data exists for symbol and date
     */
    boolean existsBySymbolAndDate(String symbol, LocalDate date);
    
    // ==========================================
    // CUSTOM ANALYSIS QUERIES
    // ==========================================
    
    /**
     * Find stocks that had consecutive positive/negative days
     */
    @Query("{'symbol': ?0, 'date': {$gte: ?1, $lte: ?2}}")
    List<StockData> findForTrendAnalysis(String symbol, LocalDate startDate, LocalDate endDate);
    
    /**
     * Delete old data (cleanup)
     */
    void deleteByDateBefore(LocalDate cutoffDate);
    
    /**
     * Delete data for specific symbol
     */
    void deleteBySymbol(String symbol);

        /**
     * Count distinct dates
     */
    @Query("db.stock_data.distinct('date').length")
    
        /**
     * Count distinct dates using aggregation
     */
    @Aggregation(pipeline = {
        "{ '$group': { '_id': '$date' } }",
        "{ '$count': 'totalDates' }"
    })
    Long countDistinctDates();

    /**
     * Find by date greater than or equal
     */
    List<StockData> findByDateGreaterThanEqual(LocalDate date);

    /**
     * Find by symbol and date greater than or equal
     */
    List<StockData> findBySymbolAndDateGreaterThanEqual(String symbol, LocalDate date);

    /**
     * Find by date range
     */
    List<StockData> findByDateBetween(LocalDate fromDate, LocalDate toDate);

    /**
     * Find with moving averages after date
     */
    List<StockData> findByDateGreaterThanEqualAndMovingAverage100DayIsNotNull(LocalDate date);

    /**
     * Find by date and percentage change greater than
     */
    List<StockData> findByDateGreaterThanEqualAndPercentageChangeGreaterThanEqual(LocalDate date, BigDecimal percentage);

    /**
     * Find by date and percentage change less than
     */
    List<StockData> findByDateGreaterThanEqualAndPercentageChangeLessThanEqual(LocalDate date, BigDecimal percentage);

    

}