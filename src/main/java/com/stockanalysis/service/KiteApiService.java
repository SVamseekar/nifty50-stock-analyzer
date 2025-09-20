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
 * FIXED: Service to handle Kite API data fetching operations
 * - Correct API endpoint paths
 * - Proper error handling
 * - Better response parsing
 * - Optimized rate limiting
 */
@Service
public class KiteApiService {
    
    private static final Logger logger = LoggerFactory.getLogger(KiteApiService.class);
    
    @Autowired
    private WebClient kiteWebClient;
    
    @Autowired
    private KiteAuthService kiteAuthService;
    
    @Value("${kite.api.key}")
    private String apiKey;
    
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    /**
     * FIXED: Fetch historical data with correct Kite API endpoint and response handling
     */
    public JsonNode fetchHistoricalData(String instrumentToken, LocalDate fromDate, LocalDate toDate) {
        try {
            if (!kiteAuthService.isAuthenticated()) {
                logger.error("❌ Kite API not authenticated. Please authenticate first.");
                return null;
            }
            
            // FIXED: Correct Kite API endpoint format
            String endpoint = String.format("/instruments/historical/%s/%s", instrumentToken, "day");
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            
            logger.debug("🔍 Fetching historical data: {} from {} to {}", instrumentToken, fromDate, toDate);
            logger.debug("📡 API Endpoint: {}", endpoint);
            
            // FIXED: Get current access token
            String accessToken = kiteAuthService.getCurrentAccessToken();
            if (accessToken == null || accessToken.trim().isEmpty()) {
                logger.error("❌ Access token is null or empty");
                return null;
            }
            
            // FIXED: Correct authorization header format
            String authHeader = "token " + apiKey + ":" + accessToken;
            logger.debug("🔐 Auth header: token {}:{}****", apiKey.substring(0, 4), accessToken.substring(0, 4));
            
            String response = kiteWebClient
                .get()
                .uri(uriBuilder -> uriBuilder
                    .path(endpoint)
                    .queryParam("from", fromDate.format(formatter))
                    .queryParam("to", toDate.format(formatter))
                    .build())
                .header("Authorization", authHeader)
                .header("X-Kite-Version", "3")
                .header("User-Agent", "StockAnalyzer/3.0")
                .retrieve()
                .bodyToMono(String.class)
                .block();
            
            if (response == null || response.trim().isEmpty()) {
                logger.warn("⚠️ Empty response from Kite API for instrument {}", instrumentToken);
                return null;
            }
            
            logger.debug("📨 Raw response length: {} chars", response.length());
            logger.debug("📨 Response preview: {}...", response.substring(0, Math.min(200, response.length())));
            
            JsonNode jsonResponse = objectMapper.readTree(response);
            
            // FIXED: Proper response validation
            if (!jsonResponse.has("status")) {
                logger.error("❌ Invalid response format - missing status field for {}", instrumentToken);
                logger.debug("🔍 Full response: {}", response);
                return null;
            }
            
            String status = jsonResponse.get("status").asText();
            
            if ("success".equals(status)) {
                if (!jsonResponse.has("data")) {
                    logger.warn("⚠️ Success response but no data field for {}", instrumentToken);
                    return null;
                }
                
                JsonNode data = jsonResponse.get("data");
                
                // FIXED: Handle both direct array and nested structure
                JsonNode candles = null;
                if (data.isArray()) {
                    candles = data; // Direct array format
                } else if (data.has("candles")) {
                    candles = data.get("candles"); // Nested format
                } else {
                    logger.warn("⚠️ No candles data found in response for {}", instrumentToken);
                    logger.debug("🔍 Data structure: {}", data);
                    return null;
                }
                
                if (candles == null || candles.size() == 0) {
                    logger.warn("⚠️ Empty candles array for {} (period: {} to {})", 
                              instrumentToken, fromDate, toDate);
                    return null;
                }
                
                logger.debug("✅ Successfully fetched {} candles for instrument {}", 
                           candles.size(), instrumentToken);
                return candles;
                
            } else {
                // Handle error responses
                String errorMsg = jsonResponse.has("message") ? 
                    jsonResponse.get("message").asText() : "Unknown error";
                String errorType = jsonResponse.has("error_type") ? 
                    jsonResponse.get("error_type").asText() : "Unknown";
                
                logger.error("❌ Kite API error for {}: {} (Type: {})", instrumentToken, errorMsg, errorType);
                
                // Specific error handling
                if ("TokenException".equals(errorType)) {
                    logger.error("🔐 Token expired or invalid - need to re-authenticate");
                } else if ("NetworkException".equals(errorType)) {
                    logger.error("🌐 Network error - check connectivity");
                } else if ("DataException".equals(errorType)) {
                    logger.warn("📊 No data available for the requested period");
                }
                
                return null;
            }
            
        } catch (WebClientResponseException e) {
            logger.error("🌐 HTTP {} error fetching data for {}: {}", 
                        e.getStatusCode(), instrumentToken, e.getResponseBodyAsString());
            
            // Enhanced error handling based on status codes
            switch (e.getStatusCode().value()) {
                case 403:
                    logger.error("🔐 Access forbidden - token may be expired or invalid");
                    break;
                case 400:
                    logger.error("📝 Bad request - check instrument token and date format");
                    break;
                case 429:
                    logger.error("⚡ Rate limit exceeded - slow down requests");
                    break;
                case 500:
                    logger.error("🔧 Kite server error - try again later");
                    break;
            }
            
            return null;
            
        } catch (Exception e) {
            logger.error("💥 Unexpected error fetching data for {}: {}", instrumentToken, e.getMessage(), e);
            return null;
        }
    }
    
    /**
     * FIXED: Optimized rate limiting for Kite API (3 req/sec limit)
     */
    public void rateLimit() {
        try {
            // FIXED: 350ms delay = ~2.8 req/sec (safe buffer under 3 req/sec limit)
            Thread.sleep(350);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("⚠️ Rate limiting interrupted");
        }
    }
    
    /**
     * FIXED: Fetch instruments list with proper error handling
     */
    public String fetchInstruments(String exchange) {
        try {
            if (!kiteAuthService.isAuthenticated()) {
                logger.error("❌ Kite API not authenticated for instruments fetch");
                return null;
            }
            
            logger.debug("🔍 Fetching instruments for exchange: {}", exchange);
            
            String accessToken = kiteAuthService.getCurrentAccessToken();
            String authHeader = "token " + apiKey + ":" + accessToken;
            
            String response = kiteWebClient
                .get()
                .uri("/instruments/" + exchange)
                .header("Authorization", authHeader)
                .header("X-Kite-Version", "3")
                .retrieve()
                .bodyToMono(String.class)
                .block();
            
            if (response != null && !response.trim().isEmpty()) {
                logger.debug("✅ Successfully fetched instruments for {} ({} chars)", 
                           exchange, response.length());
                return response; // Returns CSV format
            } else {
                logger.warn("⚠️ Empty instruments response for {}", exchange);
                return null;
            }
            
        } catch (WebClientResponseException e) {
            logger.error("🌐 HTTP {} error fetching instruments for {}: {}", 
                        e.getStatusCode(), exchange, e.getResponseBodyAsString());
            return null;
            
        } catch (Exception e) {
            logger.error("💥 Unexpected error fetching instruments for {}: {}", exchange, e.getMessage());
            return null;
        }
    }
    
    /**
     * FIXED: Fetch user profile with enhanced error handling
     */
    public JsonNode fetchProfile() {
        try {
            if (!kiteAuthService.isAuthenticated()) {
                logger.error("❌ Kite API not authenticated for profile fetch");
                return null;
            }
            
            logger.debug("🔍 Fetching user profile");
            
            String accessToken = kiteAuthService.getCurrentAccessToken();
            String authHeader = "token " + apiKey + ":" + accessToken;
            
            String response = kiteWebClient
                .get()
                .uri("/user/profile")
                .header("Authorization", authHeader)
                .header("X-Kite-Version", "3")
                .retrieve()
                .bodyToMono(String.class)
                .block();
            
            if (response == null || response.trim().isEmpty()) {
                logger.warn("⚠️ Empty profile response");
                return null;
            }
            
            JsonNode jsonResponse = objectMapper.readTree(response);
            
            if (jsonResponse.has("status") && "success".equals(jsonResponse.get("status").asText())) {
                logger.debug("✅ Successfully fetched user profile");
                return jsonResponse.get("data");
            } else {
                String error = jsonResponse.has("message") ? 
                    jsonResponse.get("message").asText() : "Unknown error";
                logger.error("❌ Profile fetch failed: {}", error);
                return null;
            }
            
        } catch (WebClientResponseException e) {
            logger.error("🌐 HTTP {} error fetching profile: {}", 
                        e.getStatusCode(), e.getResponseBodyAsString());
            return null;
            
        } catch (Exception e) {
            logger.error("💥 Unexpected error fetching profile: {}", e.getMessage());
            return null;
        }
    }
    
    /**
     * FIXED: Fetch quotes with proper request formatting
     */
    public JsonNode fetchQuotes(List<String> instruments) {
        try {
            if (!kiteAuthService.isAuthenticated()) {
                logger.error("❌ Kite API not authenticated for quotes fetch");
                return null;
            }
            
            if (instruments == null || instruments.isEmpty()) {
                logger.warn("⚠️ No instruments provided for quotes fetch");
                return null;
            }
            
            logger.debug("🔍 Fetching quotes for {} instruments", instruments.size());
            
            String accessToken = kiteAuthService.getCurrentAccessToken();
            String authHeader = "token " + apiKey + ":" + accessToken;
            
            // FIXED: Properly format instruments parameter
            String instrumentsParam = String.join(",", instruments);
            
            String response = kiteWebClient
                .get()
                .uri(uriBuilder -> uriBuilder
                    .path("/quote")
                    .queryParam("i", instrumentsParam)
                    .build())
                .header("Authorization", authHeader)
                .header("X-Kite-Version", "3")
                .retrieve()
                .bodyToMono(String.class)
                .block();
            
            if (response == null || response.trim().isEmpty()) {
                logger.warn("⚠️ Empty quotes response");
                return null;
            }
            
            JsonNode jsonResponse = objectMapper.readTree(response);
            
            if (jsonResponse.has("status") && "success".equals(jsonResponse.get("status").asText())) {
                logger.debug("✅ Successfully fetched quotes");
                return jsonResponse.get("data");
            } else {
                String error = jsonResponse.has("message") ? 
                    jsonResponse.get("message").asText() : "Unknown error";
                logger.error("❌ Quotes fetch failed: {}", error);
                return null;
            }
            
        } catch (WebClientResponseException e) {
            logger.error("🌐 HTTP {} error fetching quotes: {}", 
                        e.getStatusCode(), e.getResponseBodyAsString());
            return null;
            
        } catch (Exception e) {
            logger.error("💥 Unexpected error fetching quotes: {}", e.getMessage());
            return null;
        }
    }
    
    /**
     * Enhanced connection test with detailed feedback
     */
    public boolean testConnection() {
        try {
            logger.info("🔍 Testing Kite API connection...");
            
            JsonNode profile = fetchProfile();
            if (profile != null) {
                String userName = profile.has("user_name") ? 
                    profile.get("user_name").asText() : "Unknown";
                logger.info("✅ Kite API connection test successful - User: {}", userName);
                return true;
            } else {
                logger.warn("⚠️ API connection test failed - no profile data returned");
                return false;
            }
            
        } catch (Exception e) {
            logger.error("❌ API connection test failed: {}", e.getMessage());
            return false;
        }
    }
    
    /**
     * FIXED: Check if market is open (enhanced logic)
     */
    public boolean isMarketOpen() {
        try {
            LocalDate today = LocalDate.now();
            int dayOfWeek = today.getDayOfWeek().getValue();
            
            // Market is closed on weekends (Saturday=6, Sunday=7)
            if (dayOfWeek >= 6) {
                logger.debug("📅 Market closed - Weekend (day: {})", dayOfWeek);
                return false;
            }
            
            // TODO: Add holiday checking logic here
            // You could fetch holidays from Kite API and cache them
            
            logger.debug("📅 Market should be open (day: {})", dayOfWeek);
            return true;
            
        } catch (Exception e) {
            logger.error("❌ Error checking market status: {}", e.getMessage());
            return false;
        }
    }
    
    /**
     * Enhanced instrument token validation
     */
    public boolean isValidInstrumentToken(String instrumentToken) {
        if (instrumentToken == null || instrumentToken.trim().isEmpty()) {
            logger.warn("⚠️ Instrument token is null or empty");
            return false;
        }
        
        try {
            // Instrument tokens are typically numeric strings
            long tokenValue = Long.parseLong(instrumentToken);
            if (tokenValue <= 0) {
                logger.warn("⚠️ Invalid instrument token value: {}", tokenValue);
                return false;
            }
            return true;
        } catch (NumberFormatException e) {
            logger.warn("⚠️ Invalid instrument token format: {}", instrumentToken);
            return false;
        }
    }
    
    /**
     * Get API usage statistics for monitoring
     */
    public Map<String, Object> getApiUsageStats() {
        Map<String, Object> stats = new HashMap<>();
        
        try {
            boolean authenticated = kiteAuthService.isAuthenticated();
            boolean connectionHealthy = authenticated ? testConnection() : false;
            
            stats.put("authenticated", authenticated);
            stats.put("connectionHealthy", connectionHealthy);
            stats.put("rateLimitDelay", 350);
            stats.put("maxRequestsPerSecond", "2.8 (safe under 3/sec limit)");
            stats.put("apiKey", apiKey != null ? apiKey.substring(0, 4) + "****" : "NOT_SET");
            stats.put("authHeaderFormat", "token api_key:access_token");
            stats.put("timestamp", java.time.LocalDateTime.now());
            
            if (authenticated) {
                String token = kiteAuthService.getCurrentAccessToken();
                stats.put("tokenPreview", token != null ? token.substring(0, 4) + "****" : "NULL");
            }
            
        } catch (Exception e) {
            logger.error("❌ Error getting API usage stats: {}", e.getMessage());
            stats.put("error", e.getMessage());
        }
        
        return stats;
    }
    
    /**
     * FIXED: Fetch trading holidays with proper error handling
     */
    public JsonNode fetchHolidays() {
        try {
            if (!kiteAuthService.isAuthenticated()) {
                logger.error("❌ Kite API not authenticated for holidays fetch");
                return null;
            }
            
            logger.debug("🔍 Fetching trading holidays");
            
            String accessToken = kiteAuthService.getCurrentAccessToken();
            String authHeader = "token " + apiKey + ":" + accessToken;
            
            String response = kiteWebClient
                .get()
                .uri("/market/holidays")
                .header("Authorization", authHeader)
                .header("X-Kite-Version", "3")
                .retrieve()
                .bodyToMono(String.class)
                .block();
            
            if (response == null || response.trim().isEmpty()) {
                logger.warn("⚠️ Empty holidays response");
                return null;
            }
            
            JsonNode jsonResponse = objectMapper.readTree(response);
            
            if (jsonResponse.has("status") && "success".equals(jsonResponse.get("status").asText())) {
                logger.debug("✅ Successfully fetched holidays");
                return jsonResponse.get("data");
            } else {
                String error = jsonResponse.has("message") ? 
                    jsonResponse.get("message").asText() : "Unknown error";
                logger.error("❌ Holidays fetch failed: {}", error);
                return null;
            }
            
        } catch (Exception e) {
            logger.error("💥 Error fetching holidays: {}", e.getMessage());
            return null;
        }
    }
}