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
     * Count distinct dates using aggregation
     */
    @Aggregation(pipeline = {
        "{ '$group': { '_id': '$date' } }",
        "{ '$count': 'totalDates' }"
    })
    Long countDistinctDates();
    
    // ==========================================
    // DATE-BASED QUERIES
    // ==========================================
    
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
    
    /**
     * Find latest date for a symbol (most recent data)
     */
    @Query(value = "{'symbol': ?0}", sort = "{'date': -1}")
    Optional<StockData> findLatestBySymbol(String symbol);
    
    /**
     * Find all records for a specific date across all symbols
     */
    List<StockData> findByDate(LocalDate date);
    
    /**
     * Check if data exists for symbol and date
     */
    boolean existsBySymbolAndDate(String symbol, LocalDate date);
    
    /**
     * Count records for a symbol
     */
    long countBySymbol(String symbol);
    
    /**
     * Delete data for specific symbol
     */
    void deleteBySymbol(String symbol);
}