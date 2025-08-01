package com.stockanalysis.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import java.time.LocalDate;
import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Enhanced StockData model with complete OHLCV data from Kite API
 */
@Document(collection = "stock_data")
@CompoundIndex(name = "symbol_date_idx", def = "{'symbol' : 1, 'date' : 1}", unique = true)
public class StockData {
    
    @Id
    private String id; // MongoDB auto-generates this
    
    @Indexed
    private String symbol; // Stock symbol like "RELIANCE", "TCS"
    
    @Indexed
    private LocalDate date; // Trading date
    
    // OHLCV Data from Kite API
    private BigDecimal openPrice;      // Opening price
    private BigDecimal highPrice;      // Highest price of the day
    private BigDecimal lowPrice;       // Lowest price of the day
    private BigDecimal closingPrice;   // Closing price (main price)
    private Long volume;               // Trading volume
    
    // Calculated fields
    private BigDecimal percentageChange; // % change from previous day
    private BigDecimal priceChange;      // Absolute price change
    
    // Additional metadata
    private String exchange;           // NSE, BSE
    private String instrumentType;     // EQ for equity
    
    // Default constructor (required by Spring Data MongoDB)
    public StockData() {}
    
    // Main constructor
    public StockData(String symbol, LocalDate date, BigDecimal closingPrice, 
                     BigDecimal percentageChange, Long volume) {
        this.symbol = symbol;
        this.date = date;
        this.closingPrice = closingPrice;
        this.percentageChange = percentageChange;
        this.volume = volume;
        this.exchange = "NSE";          // Default to NSE
        this.instrumentType = "EQ";     // Default to equity
    }
    
    // Full constructor with OHLCV
    public StockData(String symbol, LocalDate date, BigDecimal openPrice, BigDecimal highPrice,
                     BigDecimal lowPrice, BigDecimal closingPrice, Long volume, 
                     BigDecimal percentageChange) {
        this.symbol = symbol;
        this.date = date;
        this.openPrice = openPrice;
        this.highPrice = highPrice;
        this.lowPrice = lowPrice;
        this.closingPrice = closingPrice;
        this.volume = volume;
        this.percentageChange = percentageChange;
        this.exchange = "NSE";
        this.instrumentType = "EQ";
        
        // Calculate price change
        if (percentageChange != null && closingPrice != null) {
            this.priceChange = closingPrice.multiply(percentageChange)
                .divide(BigDecimal.valueOf(100), 2, RoundingMode.HALF_UP);
        }
    }
    
    // Getters and Setters
    public String getId() {
        return id;
    }
    
    public void setId(String id) {
        this.id = id;
    }
    
    public String getSymbol() {
        return symbol;
    }
    
    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }
    
    public LocalDate getDate() {
        return date;
    }
    
    public void setDate(LocalDate date) {
        this.date = date;
    }
    
    public BigDecimal getOpenPrice() {
        return openPrice;
    }
    
    public void setOpenPrice(BigDecimal openPrice) {
        this.openPrice = openPrice;
    }
    
    public BigDecimal getHighPrice() {
        return highPrice;
    }
    
    public void setHighPrice(BigDecimal highPrice) {
        this.highPrice = highPrice;
    }
    
    public BigDecimal getLowPrice() {
        return lowPrice;
    }
    
    public void setLowPrice(BigDecimal lowPrice) {
        this.lowPrice = lowPrice;
    }
    
    public BigDecimal getClosingPrice() {
        return closingPrice;
    }
    
    public void setClosingPrice(BigDecimal closingPrice) {
        this.closingPrice = closingPrice;
    }
    
    public Long getVolume() {
        return volume;
    }
    
    public void setVolume(Long volume) {
        this.volume = volume;
    }
    
    public BigDecimal getPercentageChange() {
        return percentageChange;
    }
    
    public void setPercentageChange(BigDecimal percentageChange) {
        this.percentageChange = percentageChange;
        
        // Recalculate price change when percentage change is set
        if (percentageChange != null && closingPrice != null) {
            this.priceChange = closingPrice.multiply(percentageChange)
                .divide(BigDecimal.valueOf(100), 2, RoundingMode.HALF_UP);
        }
    }
    
    public BigDecimal getPriceChange() {
        return priceChange;
    }
    
    public void setPriceChange(BigDecimal priceChange) {
        this.priceChange = priceChange;
    }
    
    public String getExchange() {
        return exchange;
    }
    
    public void setExchange(String exchange) {
        this.exchange = exchange;
    }
    
    public String getInstrumentType() {
        return instrumentType;
    }
    
    public void setInstrumentType(String instrumentType) {
        this.instrumentType = instrumentType;
    }
    
    // Helper methods
    public boolean isSignificantChange(double threshold) {
        if (percentageChange == null) return false;
        return Math.abs(percentageChange.doubleValue()) >= threshold;
    }
    
    public boolean isPositiveChange() {
        return percentageChange != null && percentageChange.compareTo(BigDecimal.ZERO) > 0;
    }
    
    public boolean isNegativeChange() {
        return percentageChange != null && percentageChange.compareTo(BigDecimal.ZERO) < 0;
    }
    
    // Calculate daily return (using proper BigDecimal methods)
    public BigDecimal getDailyReturn() {
        if (openPrice == null || closingPrice == null || openPrice.compareTo(BigDecimal.ZERO) == 0) {
            return BigDecimal.ZERO;
        }
        return closingPrice.subtract(openPrice).divide(openPrice, 4, RoundingMode.HALF_UP);
    }
    
    // Calculate price range (high - low)
    public BigDecimal getDailyRange() {
        if (highPrice == null || lowPrice == null) {
            return BigDecimal.ZERO;
        }
        return highPrice.subtract(lowPrice);
    }
    
    @Override
    public String toString() {
        return String.format("StockData{symbol='%s', date=%s, open=%s, high=%s, low=%s, close=%s, volume=%s, change=%s%%}", 
                           symbol, date, openPrice, highPrice, lowPrice, closingPrice, volume, percentageChange);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        
        StockData stockData = (StockData) obj;
        return symbol.equals(stockData.symbol) && date.equals(stockData.date);
    }
    
    @Override
    public int hashCode() {
        return symbol.hashCode() + date.hashCode();
    }
}