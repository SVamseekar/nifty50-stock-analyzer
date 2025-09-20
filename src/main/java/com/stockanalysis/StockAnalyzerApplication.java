package com.stockanalysis;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.context.annotation.Bean;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.lang.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.annotation.PostConstruct;
import java.util.TimeZone;

@SpringBootApplication
@EnableScheduling
public class StockAnalyzerApplication {
    
    private static final Logger logger = LoggerFactory.getLogger(StockAnalyzerApplication.class);
    
    // CLEAN MAIN METHOD - NO DEVTOOLS IMPORT
    public static void main(String[] args) {
        try {
            logger.info("Starting Nifty 50 Stock Analyzer Application...");
            SpringApplication.run(StockAnalyzerApplication.class, args);
            logger.info("Nifty 50 Stock Analyzer Application started successfully!");
        } catch (Exception e) {
            // Check if it's a DevTools restart exception by class name (no import needed)
            if (e.getClass().getName().contains("SilentExitException")) {
                logger.debug("DevTools restart triggered - this is normal");
                return; // Don't exit for DevTools restarts
            }
            logger.error("Failed to start Nifty 50 Stock Analyzer Application: {}", e.getMessage(), e);
            System.exit(1);
        }
    }
    
    @PostConstruct
    public void init() {
        // Set default timezone to Indian Standard Time for stock market
        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Kolkata"));
        logger.info("Application timezone set to: {}", TimeZone.getDefault().getID());
        
        // Log application startup info
        logger.info("=== NIFTY 50 STOCK ANALYZER CONFIGURATION ===");
        logger.info("Market: Nifty 50 Stocks");
        logger.info("Scheduled jobs enabled: YES");
        logger.info("Default timezone: {}", TimeZone.getDefault().getID());
        logger.info("Server will start on: http://localhost:8081/stock-analyzer");
        logger.info("Real-time data source: Kite Connect API");
        logger.info("Database: MongoDB (stock-analyzer-ma)");
        logger.info("Features: Real API, Scheduled Jobs, Enhanced Analytics, Moving Averages");
        logger.info("===============================================");
    }
    
    @Bean
    public WebMvcConfigurer corsConfigurer() {
        return new WebMvcConfigurer() {
            @Override
            public void addCorsMappings(@NonNull CorsRegistry registry) {
                registry.addMapping("/api/**")
                        .allowedOrigins("*")
                        .allowedMethods("GET", "POST", "PUT", "DELETE", "OPTIONS")
                        .allowedHeaders("*")
                        .maxAge(3600);
                        
                logger.debug("CORS configuration applied for API endpoints");
            }
        };
    }
}