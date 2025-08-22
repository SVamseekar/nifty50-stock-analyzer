package com.stockanalysis.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Service to handle Kite API data fetching operations
 * FIXED: Correct authorization header format
 */
@Service
public class KiteApiService {
    
    private static final Logger logger = LoggerFactory.getLogger(KiteApiService.class);
    
    @Autowired
    private WebClient kiteWebClient;
    
    @Autowired
    private KiteAuthService kiteAuthService;
    
    @Value("${kite.api.key}")
    private String apiKey;  // ADDED: Need API key for authorization header
    
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    /**
     * FIXED: Fetch historical data with correct authorization header format
     * Kite requires: "token api_key:access_token"
     */
    public JsonNode fetchHistoricalData(String instrumentToken, LocalDate fromDate, LocalDate toDate) {
        try {
            if (!kiteAuthService.isAuthenticated()) {
                throw new RuntimeException("Kite API not authenticated. Please authenticate first.");
            }
            
            String url = String.format("/instruments/historical/%s/day", instrumentToken);
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            
            logger.debug("🔍 Fetching historical data: {} from {} to {}", instrumentToken, fromDate, toDate);
            
            // FIXED: Correct authorization header format
            String authHeader = "token " + apiKey + ":" + kiteAuthService.getCurrentAccessToken();
            
            String response = kiteWebClient
                .get()
                .uri(uriBuilder -> uriBuilder
                    .path(url)
                    .queryParam("from", fromDate.format(formatter))
                    .queryParam("to", toDate.format(formatter))
                    .build())
                .header("Authorization", authHeader)
                .retrieve()
                .bodyToMono(String.class)
                .block();
            
            JsonNode jsonResponse = objectMapper.readTree(response);
            
            if (jsonResponse.has("status") && "success".equals(jsonResponse.get("status").asText())) {
                logger.debug("Successfully fetched data for instrument {}", instrumentToken);
                return jsonResponse.get("data");
            } else {
                logger.error("Kite API error: {}", jsonResponse);
                throw new RuntimeException("Kite API returned error: " + jsonResponse);
            }
            
        } catch (WebClientResponseException e) {
            logger.error("HTTP error fetching data for {}: {} - {}", 
                        instrumentToken, e.getStatusCode(), e.getResponseBodyAsString());
            
            if (e.getStatusCode().value() == 403) {
                logger.error("Access token may have expired. Please re-authenticate.");
            }
            
            throw new RuntimeException("HTTP error: " + e.getStatusCode() + " - " + e.getResponseBodyAsString());
            
        } catch (Exception e) {
            logger.error("Unexpected error fetching data for {}: {}", instrumentToken, e.getMessage());
            throw new RuntimeException("Failed to fetch historical data", e);
        }
    }
    
    /**
     * FIXED: Fetch instruments list with correct authorization header
     */
    public String fetchInstruments(String exchange) {
        try {
            if (!kiteAuthService.isAuthenticated()) {
                throw new RuntimeException("Kite API not authenticated. Please authenticate first.");
            }
            
            logger.debug("🔍 Fetching instruments for exchange: {}", exchange);
            
            // FIXED: Correct authorization header format
            String authHeader = "token " + apiKey + ":" + kiteAuthService.getCurrentAccessToken();
            
            String response = kiteWebClient
                .get()
                .uri("/instruments/" + exchange)
                .header("Authorization", authHeader)
                .retrieve()
                .bodyToMono(String.class)
                .block();
            
            logger.debug("Successfully fetched instruments for {}", exchange);
            return response; // Returns CSV format
            
        } catch (WebClientResponseException e) {
            logger.error("HTTP error fetching instruments: {} - {}", 
                        e.getStatusCode(), e.getResponseBodyAsString());
            throw new RuntimeException("HTTP error: " + e.getStatusCode());
            
        } catch (Exception e) {
            logger.error("Unexpected error fetching instruments: {}", e.getMessage());
            throw new RuntimeException("Failed to fetch instruments", e);
        }
    }
    
    /**
     * FIXED: Fetch user profile with correct authorization header
     */
    public JsonNode fetchProfile() {
        try {
            if (!kiteAuthService.isAuthenticated()) {
                throw new RuntimeException("Kite API not authenticated. Please authenticate first.");
            }
            
            logger.debug("Fetching user profile");
            
            // FIXED: Correct authorization header format
            String authHeader = "token " + apiKey + ":" + kiteAuthService.getCurrentAccessToken();
            
            String response = kiteWebClient
                .get()
                .uri("/user/profile")
                .header("Authorization", authHeader)
                .retrieve()
                .bodyToMono(String.class)
                .block();
            
            JsonNode jsonResponse = objectMapper.readTree(response);
            
            if (jsonResponse.has("status") && "success".equals(jsonResponse.get("status").asText())) {
                logger.debug("Successfully fetched user profile");
                return jsonResponse.get("data");
            } else {
                throw new RuntimeException("Profile fetch failed: " + jsonResponse);
            }
            
        } catch (WebClientResponseException e) {
            logger.error("HTTP error fetching profile: {} - {}", 
                        e.getStatusCode(), e.getResponseBodyAsString());
            throw new RuntimeException("HTTP error: " + e.getStatusCode());
            
        } catch (Exception e) {
            logger.error("Unexpected error fetching profile: {}", e.getMessage());
            throw new RuntimeException("Failed to fetch profile", e);
        }
    }
    
    /**
     * FIXED: Fetch quotes with correct authorization header
     */
    public JsonNode fetchQuotes(List<String> instruments) {
        try {
            if (!kiteAuthService.isAuthenticated()) {
                throw new RuntimeException("Kite API not authenticated. Please authenticate first.");
            }
            
            logger.debug("Fetching quotes for {} instruments", instruments.size());
            
            // FIXED: Correct authorization header format
            String authHeader = "token " + apiKey + ":" + kiteAuthService.getCurrentAccessToken();
            String instrumentsParam = String.join(",", instruments);
            
            String response = kiteWebClient
                .get()
                .uri(uriBuilder -> uriBuilder
                    .path("/quote")
                    .queryParam("i", instrumentsParam)
                    .build())
                .header("Authorization", authHeader)
                .retrieve()
                .bodyToMono(String.class)
                .block();
            
            JsonNode jsonResponse = objectMapper.readTree(response);
            
            if (jsonResponse.has("status") && "success".equals(jsonResponse.get("status").asText())) {
                logger.debug("Successfully fetched quotes");
                return jsonResponse.get("data");
            } else {
                throw new RuntimeException("Quote fetch failed: " + jsonResponse);
            }
            
        } catch (WebClientResponseException e) {
            logger.error("HTTP error fetching quotes: {} - {}", 
                        e.getStatusCode(), e.getResponseBodyAsString());
            throw new RuntimeException("HTTP error: " + e.getStatusCode());
            
        } catch (Exception e) {
            logger.error("Unexpected error fetching quotes: {}", e.getMessage());
            throw new RuntimeException("Failed to fetch quotes", e);
        }
    }
    
    /**
     * Test API connectivity by fetching user profile
     */
    public boolean testConnection() {
        try {
            fetchProfile();
            logger.info("Kite API connection test successful");
            return true;
        } catch (Exception e) {
            logger.warn("API connection test failed: {}", e.getMessage());
            return false;
        }
    }
    
    /**
     * Rate limiting helper - ensures we don't exceed Kite API limits (3 req/sec)
     */
    public void rateLimit() {
        try {
            Thread.sleep(400); // 400ms delay = ~2.5 requests per second (safe buffer)
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Rate limiting interrupted");
        }
    }
    
    /**
     * FIXED: Get trading holidays with correct authorization header
     */
    public JsonNode fetchHolidays() {
        try {
            if (!kiteAuthService.isAuthenticated()) {
                throw new RuntimeException("Kite API not authenticated. Please authenticate first.");
            }
            
            logger.debug("Fetching trading holidays");
            
            // FIXED: Correct authorization header format
            String authHeader = "token " + apiKey + ":" + kiteAuthService.getCurrentAccessToken();
            
            String response = kiteWebClient
                .get()
                .uri("/market/holidays")
                .header("Authorization", authHeader)
                .retrieve()
                .bodyToMono(String.class)
                .block();
            
            JsonNode jsonResponse = objectMapper.readTree(response);
            
            if (jsonResponse.has("status") && "success".equals(jsonResponse.get("status").asText())) {
                logger.debug("Successfully fetched holidays");
                return jsonResponse.get("data");
            } else {
                throw new RuntimeException("Holidays fetch failed: " + jsonResponse);
            }
            
        } catch (Exception e) {
            logger.error("Error fetching holidays: {}", e.getMessage());
            throw new RuntimeException("Failed to fetch holidays", e);
        }
    }
    
    /**
     * Check if market is open
     */
    public boolean isMarketOpen() {
        try {
            // This is a simplified check - in production you might want to 
            // call the actual market status API or check against holidays
            LocalDate today = LocalDate.now();
            int dayOfWeek = today.getDayOfWeek().getValue();
            
            // Market is closed on weekends (Saturday=6, Sunday=7)
            if (dayOfWeek >= 6) {
                return false;
            }
            
            // You could enhance this by checking actual market hours and holidays
            return true;
            
        } catch (Exception e) {
            logger.error("Error checking market status: {}", e.getMessage());
            return false;
        }
    }
    
    /**
     * Validate instrument token format
     */
    public boolean isValidInstrumentToken(String instrumentToken) {
        if (instrumentToken == null || instrumentToken.trim().isEmpty()) {
            return false;
        }
        
        try {
            // Instrument tokens are typically numeric strings
            Long.parseLong(instrumentToken);
            return true;
        } catch (NumberFormatException e) {
            logger.warn("Invalid instrument token format: {}", instrumentToken);
            return false;
        }
    }
    
    /**
     * FIXED: Build authorization header for API requests
     */
    public String getAuthorizationHeader() {
        if (!kiteAuthService.isAuthenticated()) {
            throw new RuntimeException("Not authenticated");
        }
        // FIXED: Return correct format
        return "token " + apiKey + ":" + kiteAuthService.getCurrentAccessToken();
    }
    
    /**
     * Get API usage statistics (for monitoring)
     */
    public Map<String, Object> getApiUsageStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("authenticated", kiteAuthService.isAuthenticated());
        stats.put("rateLimitDelay", 400);
        stats.put("maxRequestsPerSecond", 3);
        stats.put("connectionHealthy", testConnection());
        stats.put("timestamp", java.time.LocalDateTime.now());
        stats.put("apiKey", apiKey != null ? apiKey.substring(0, 4) + "****" : "NOT_SET");
        stats.put("authHeaderFormat", "token api_key:access_token");
        
        return stats;
    }
}