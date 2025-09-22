package com.stockanalysis.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import java.time.LocalDate;
import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Enhanced StockData model with Moving Averages
 * REMOVED: Individual MA signals (maSignal50, maSignal100, maSignal200)
 * KEPT: Golden Cross and Trading Signal Strength
 */
@Document(collection = "stock_data")
@CompoundIndex(name = "symbol_date_idx", def = "{'symbol' : 1, 'date' : 1}", unique = true)
public class StockData {
    
    @Id
    private String id;
    
    @Indexed
    private String symbol;
    
    @Indexed
    private LocalDate date;
    
    // OHLCV Data
    @Field("open_price")
    private BigDecimal openPrice;
    
    @Field("high_price")
    private BigDecimal highPrice;
    
    @Field("low_price")
    private BigDecimal lowPrice;
    
    @Field("closing_price")
    private BigDecimal closingPrice;
    
    private Long volume;
    
    // Calculated fields
    @Field("percentage_change")
    private BigDecimal percentageChange;
    
    @Field("price_change")
    private BigDecimal priceChange;
    
    // Moving Averages (keeping these)
    private BigDecimal movingAverage50Day;
    private BigDecimal movingAverage100Day;
    private BigDecimal movingAverage200Day;
    
    // KEPT: Golden Cross only
    private String goldenCross;  // "GOLDEN_CROSS", "DEATH_CROSS", "NONE"

    // Metadata
    private String exchange;
    
    @Field("instrument_type")
    private String instrumentType;
    
    @Field("created_at")
    private LocalDate createdAt;
    
    @Field("updated_at")
    private LocalDate updatedAt;
    
    // Constructors
    public StockData() {
        this.createdAt = LocalDate.now();
        this.updatedAt = LocalDate.now();
    }
    
    public StockData(String symbol, LocalDate date, BigDecimal closingPrice, 
                     BigDecimal percentageChange, Long volume) {
        this();
        this.symbol = symbol;
        this.date = date;
        this.closingPrice = closingPrice;
        this.percentageChange = percentageChange;
        this.volume = volume;
        this.exchange = "NSE";
        this.instrumentType = "EQ";
    }
    
    public StockData(String symbol, LocalDate date, BigDecimal openPrice, BigDecimal highPrice,
                     BigDecimal lowPrice, BigDecimal closingPrice, Long volume, 
                     BigDecimal percentageChange) {
        this();
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
        
        calculatePriceChange();
    }
    
    // Helper Methods
    public boolean canCalculate50DayMA() {
        return movingAverage50Day != null;
    }
    
    public boolean canCalculate100DayMA() {
        return movingAverage100Day != null;
    }
    
    public boolean canCalculate200DayMA() {
        return movingAverage200Day != null;
    }
    
    /**
     * Calculate Golden Cross / Death Cross signal
     */
    public void calculateCrossSignal() {
        if (movingAverage100Day != null && movingAverage200Day != null) {
            int comparison = movingAverage100Day.compareTo(movingAverage200Day);
            
            if (comparison > 0) {
                this.goldenCross = "GOLDEN_CROSS";  // 100-day > 200-day (bullish)
            } else if (comparison < 0) {
                this.goldenCross = "DEATH_CROSS";   // 100-day < 200-day (bearish)
            } else {
                this.goldenCross = "NONE";          // Equal or very close
            }
        } else {
            this.goldenCross = "INSUFFICIENT_DATA";
        }
    }
    
    /**
     * Calculate price change from percentage change
     */
    private void calculatePriceChange() {
        if (percentageChange != null && closingPrice != null) {
            this.priceChange = closingPrice.multiply(percentageChange)
                .divide(BigDecimal.valueOf(100), 2, RoundingMode.HALF_UP);
        }
    }
    
    /**
     * Helper method to check if this is a significant moving average crossover
     */
    public boolean isSignificantCrossover() {
        return "GOLDEN_CROSS".equals(goldenCross) || "DEATH_CROSS".equals(goldenCross);
    }
    
    /**
     * Get trading signal strength (for advanced analysis)
     */
    public String getTradingSignalStrength() {
        // REMOVED: Trading signal strength functionality
        return null;
    }

    public void setTradingSignalStrength(String tradingSignalStrength) { 
        // REMOVED: Trading signal strength functionality
    }
    
    // Getters and Setters
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    
    public String getSymbol() { return symbol; }
    public void setSymbol(String symbol) { this.symbol = symbol; }
    
    public LocalDate getDate() { return date; }
    public void setDate(LocalDate date) { this.date = date; }
    
    public BigDecimal getOpenPrice() { return openPrice; }
    public void setOpenPrice(BigDecimal openPrice) { this.openPrice = openPrice; }
    
    public BigDecimal getHighPrice() { return highPrice; }
    public void setHighPrice(BigDecimal highPrice) { this.highPrice = highPrice; }
    
    public BigDecimal getLowPrice() { return lowPrice; }
    public void setLowPrice(BigDecimal lowPrice) { this.lowPrice = lowPrice; }
    
    public BigDecimal getClosingPrice() { return closingPrice; }
    public void setClosingPrice(BigDecimal closingPrice) { 
        this.closingPrice = closingPrice;
        calculatePriceChange();
        calculateCrossSignal();
    }
    
    public Long getVolume() { return volume; }
    public void setVolume(Long volume) { this.volume = volume; }
    
    public BigDecimal getPercentageChange() { return percentageChange; }
    public void setPercentageChange(BigDecimal percentageChange) { 
        this.percentageChange = percentageChange;
        calculatePriceChange();
    }
    
    public BigDecimal getPriceChange() { return priceChange; }
    public void setPriceChange(BigDecimal priceChange) { this.priceChange = priceChange; }
    
    // Moving Average Getters/Setters
    public BigDecimal getMovingAverage50Day() { return movingAverage50Day; }
    public void setMovingAverage50Day(BigDecimal movingAverage50Day) { 
        this.movingAverage50Day = movingAverage50Day;
        calculateCrossSignal();
    }
    
    public BigDecimal getMovingAverage100Day() { return movingAverage100Day; }
    public void setMovingAverage100Day(BigDecimal movingAverage100Day) { 
        this.movingAverage100Day = movingAverage100Day;
        calculateCrossSignal();
    }
    
    public BigDecimal getMovingAverage200Day() { return movingAverage200Day; }
    public void setMovingAverage200Day(BigDecimal movingAverage200Day) { 
        this.movingAverage200Day = movingAverage200Day;
        calculateCrossSignal();
    }
    
    // KEPT: Advanced signals
    public String getGoldenCross() { return goldenCross; }
    public void setGoldenCross(String goldenCross) { this.goldenCross = goldenCross; }
    
    public String getExchange() { return exchange; }
    public void setExchange(String exchange) { this.exchange = exchange; }
    
    public String getInstrumentType() { return instrumentType; }
    public void setInstrumentType(String instrumentType) { this.instrumentType = instrumentType; }
    
    public LocalDate getCreatedAt() { return createdAt; }
    public void setCreatedAt(LocalDate createdAt) { this.createdAt = createdAt; }
    
    public LocalDate getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(LocalDate updatedAt) { this.updatedAt = updatedAt; }
    
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
    
    public BigDecimal getDailyReturn() {
        if (openPrice == null || closingPrice == null || openPrice.compareTo(BigDecimal.ZERO) == 0) {
            return BigDecimal.ZERO;
        }
        return closingPrice.subtract(openPrice).divide(openPrice, 4, RoundingMode.HALF_UP);
    }
    
    public BigDecimal getDailyRange() {
        if (highPrice == null || lowPrice == null) {
            return BigDecimal.ZERO;
        }
        return highPrice.subtract(lowPrice);
    }
    
    @Override
    public String toString() {
        return String.format("StockData{symbol='%s', date=%s, close=%s, change=%s%%, ma50=%s, ma100=%s, ma200=%s, goldenCross=%s}", 
                           symbol, date, closingPrice, percentageChange, 
                           movingAverage50Day, movingAverage100Day, movingAverage200Day, goldenCross);
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