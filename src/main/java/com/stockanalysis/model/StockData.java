package com.stockanalysis.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import java.time.LocalDate;
import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Enhanced StockData model with Moving Averages support
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
    private BigDecimal openPrice;
    private BigDecimal highPrice;
    private BigDecimal lowPrice;
    private BigDecimal closingPrice;
    private Long volume;
    
    // Calculated fields
    private BigDecimal percentageChange;
    private BigDecimal priceChange;
    
    // NEW: Moving Averages
    private BigDecimal movingAverage100Day;  // 100-day MA
    private BigDecimal movingAverage200Day;  // 200-day MA
    
    // NEW: Moving Average Signals
    private String maSignal100;  // "BUY", "SELL", "HOLD", "INSUFFICIENT_DATA"
    private String maSignal200;  // "BUY", "SELL", "HOLD", "INSUFFICIENT_DATA"
    private String goldenCross;  // "GOLDEN_CROSS", "DEATH_CROSS", "NONE"
    
    // Metadata
    private String exchange;
    private String instrumentType;
    private LocalDate createdAt;
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
    
    // Full constructor with OHLCV
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
    
    // Helper Methods for Moving Averages
    
    /**
     * Check if 100-day MA can be calculated (has enough historical data)
     */
    public boolean canCalculate100DayMA() {
        return movingAverage100Day != null;
    }
    
    /**
     * Check if 200-day MA can be calculated (has enough historical data)
     */
    public boolean canCalculate200DayMA() {
        return movingAverage200Day != null;
    }
    
    /**
     * Calculate Golden Cross / Death Cross signal
     * Golden Cross: 50-day MA crosses above 200-day MA (bullish)
     * Death Cross: 50-day MA crosses below 200-day MA (bearish)
     * 
     * For simplicity, we'll use 100-day vs 200-day MA
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
     * Generate buy/sell signal based on current price vs moving average
     */
    public void calculateMASignals() {
        if (closingPrice != null) {
            // 100-day MA signal
            if (movingAverage100Day != null) {
                int comparison100 = closingPrice.compareTo(movingAverage100Day);
                if (comparison100 > 0) {
                    this.maSignal100 = "BUY";   // Price above 100-day MA
                } else if (comparison100 < 0) {
                    this.maSignal100 = "SELL";  // Price below 100-day MA
                } else {
                    this.maSignal100 = "HOLD";  // Price at 100-day MA
                }
            } else {
                this.maSignal100 = "INSUFFICIENT_DATA";
            }
            
            // 200-day MA signal
            if (movingAverage200Day != null) {
                int comparison200 = closingPrice.compareTo(movingAverage200Day);
                if (comparison200 > 0) {
                    this.maSignal200 = "BUY";   // Price above 200-day MA
                } else if (comparison200 < 0) {
                    this.maSignal200 = "SELL";  // Price below 200-day MA
                } else {
                    this.maSignal200 = "HOLD";  // Price at 200-day MA
                }
            } else {
                this.maSignal200 = "INSUFFICIENT_DATA";
            }
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
        if (!canCalculate100DayMA() || !canCalculate200DayMA()) {
            return "INSUFFICIENT_DATA";
        }
        
        boolean bullish100 = "BUY".equals(maSignal100);
        boolean bullish200 = "BUY".equals(maSignal200);
        boolean goldenCrossActive = "GOLDEN_CROSS".equals(goldenCross);
        
        if (bullish100 && bullish200 && goldenCrossActive) {
            return "STRONG_BUY";
        } else if (bullish100 && bullish200) {
            return "BUY";
        } else if (!bullish100 && !bullish200 && "DEATH_CROSS".equals(goldenCross)) {
            return "STRONG_SELL";
        } else if (!bullish100 && !bullish200) {
            return "SELL";
        } else {
            return "HOLD";
        }
    }
    
    // Getters and Setters (keeping existing ones and adding new ones)
    
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
        calculateMASignals();
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
    
    // NEW: Moving Average Getters/Setters
    public BigDecimal getMovingAverage100Day() { return movingAverage100Day; }
    public void setMovingAverage100Day(BigDecimal movingAverage100Day) { 
        this.movingAverage100Day = movingAverage100Day;
        calculateMASignals();
        calculateCrossSignal();
    }
    
    public BigDecimal getMovingAverage200Day() { return movingAverage200Day; }
    public void setMovingAverage200Day(BigDecimal movingAverage200Day) { 
        this.movingAverage200Day = movingAverage200Day;
        calculateMASignals();
        calculateCrossSignal();
    }
    
    public String getMaSignal100() { return maSignal100; }
    public void setMaSignal100(String maSignal100) { this.maSignal100 = maSignal100; }
    
    public String getMaSignal200() { return maSignal200; }
    public void setMaSignal200(String maSignal200) { this.maSignal200 = maSignal200; }
    
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
    
    // Existing helper methods (keeping all of them)
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
        return String.format("StockData{symbol='%s', date=%s, close=%s, change=%s%%, ma100=%s, ma200=%s, signal=%s}", 
                           symbol, date, closingPrice, percentageChange, 
                           movingAverage100Day, movingAverage200Day, getTradingSignalStrength());
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