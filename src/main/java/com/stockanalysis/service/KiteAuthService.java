package com.stockanalysis.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import org.springframework.http.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.concurrent.atomic.AtomicReference;
import java.nio.charset.StandardCharsets;

@Service
public class KiteAuthService {
    
    private static final Logger logger = LoggerFactory.getLogger(KiteAuthService.class);
    
    @Value("${kite.api.key}")
    private String apiKey;
    
    @Value("${kite.api.secret}")
    private String apiSecret;
    
    @Value("${server.servlet.context-path:/stock-analyzer}")
    private String contextPath;
    
    @Value("${server.port:8081}")
    private String serverPort;
    
    private final WebClient webClient;
    private final ObjectMapper objectMapper;
    private final AtomicReference<String> currentAccessToken = new AtomicReference<>();
    
    // File to store access token securely
    private static final String TOKEN_FILE = "config/kite_token.txt";
    
    public KiteAuthService() {
        this.webClient = WebClient.builder()
            .baseUrl("https://api.kite.trade")
            .defaultHeader("X-Kite-Version", "3")
            .defaultHeader("User-Agent", "StockAnalyzer/2.0")
            .build();
        this.objectMapper = new ObjectMapper();
        
        // Load existing token on startup
        loadStoredToken();
    }
    
    /**
     * Generate Kite login URL for OAuth flow
     */
    public String getKiteLoginUrl() {
        if (apiKey == null || apiKey.equals("not_configured")) {
            throw new IllegalStateException("Kite API key not configured. Please set KITE_API_KEY environment variable.");
        }
        
        String redirectUri = String.format("http://localhost:%s/api/auth/kite/callback", serverPort);
         String loginUrl = String.format("https://kite.trade/connect/login?api_key=%s&redirect_url=%s", apiKey, redirectUri);
        
        logger.info("Generated Kite login URL with redirect: {}", redirectUri);
        logger.debug("Using API Key: {}", apiKey.substring(0, 4) + "****");
        
        return loginUrl;
    }
    
    /**
     * FIXED: Exchange request_token for access_token with CORRECT checksum calculation
     */
    public String generateAccessToken(String requestToken) {
        try {
            if (apiKey == null || apiSecret == null || 
                apiKey.equals("not_configured") || apiSecret.equals("not_configured")) {
                throw new IllegalStateException("Kite API credentials not configured properly");
            }
            
            logger.info("Exchanging request_token for access_token");
            logger.debug("API Key: {}", apiKey.substring(0, 4) + "****");
            logger.debug("API Secret: {}", apiSecret.substring(0, 4) + "****");
            logger.debug("Request Token: {}", requestToken.substring(0, 8) + "****");
            
            // CORRECT CHECKSUM CALCULATION
            // Format: SHA256(api_key + request_token + api_secret)
            String checksumData = apiKey + requestToken + apiSecret;
            String checksum = generateSHA256Hash(checksumData);
            
            logger.debug("Checksum input: {}{}****", apiKey.substring(0, 4), requestToken.substring(0, 4));
            logger.debug("Generated SHA256 checksum: {}", checksum.substring(0, 8) + "****");
            
            // Make POST request using proper form data
            String response = webClient
                .post()
                .uri("/session/token")
                .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                .bodyValue(String.format("api_key=%s&request_token=%s&checksum=%s", 
                          apiKey, requestToken, checksum))
                .retrieve()
                .bodyToMono(String.class)
                .block();
            
            logger.debug("Received response from Kite API");
            
            // Parse response to extract access_token
            JsonNode responseJson = objectMapper.readTree(response);
            
            if (responseJson.has("data") && responseJson.get("data").has("access_token")) {
                String accessToken = responseJson.get("data").get("access_token").asText();
                logger.info("SUCCESS! Generated access token: {}****", accessToken.substring(0, 8));
                
                // Store the token
                storeAccessToken(accessToken);
                
                return accessToken;
            } else {
                String error = responseJson.has("message") ? responseJson.get("message").asText() : "Unknown error";
                String errorType = responseJson.has("error_type") ? responseJson.get("error_type").asText() : "Unknown";
                logger.error("Kite API error - Type: {}, Message: {}", errorType, error);
                throw new RuntimeException("Kite API error: " + error);
            }
            
        } catch (WebClientResponseException e) {
            logger.error("HTTP {} error from Kite API: {}", e.getStatusCode(), e.getResponseBodyAsString());
            
            // Enhanced error handling
            try {
                JsonNode errorJson = objectMapper.readTree(e.getResponseBodyAsString());
                String errorMessage = errorJson.has("message") ? errorJson.get("message").asText() : "Unknown error";
                String errorType = errorJson.has("error_type") ? errorJson.get("error_type").asText() : "Unknown";
                
                logger.error("Kite API Error Details:");
                logger.error("   - Error Type: {}", errorType);
                logger.error("   - Message: {}", errorMessage);
                logger.error("   - HTTP Status: {}", e.getStatusCode());
                
                // Specific troubleshooting for checksum errors
                if (errorMessage.contains("checksum") || errorMessage.contains("Invalid")) {
                    logger.error("CHECKSUM ERROR TROUBLESHOOTING:");
                    logger.error("   1. Verify your API secret is EXACTLY correct (no extra spaces)");
                    logger.error("   2. Check if you're using the correct Kite Connect app");
                    logger.error("   3. Make sure request_token hasn't expired (get fresh one)");
                    logger.error("   4. Verify API key matches the one in Kite Connect dashboard");
                    
                    // Log the exact checksum input for debugging
                    logger.error("DEBUG: If this persists, your API secret might be wrong");
                    logger.error("   Expected format: api_key + request_token + api_secret");
                    logger.error("   Your API key length: {}", apiKey.length());
                    logger.error("   Your API secret length: {}", apiSecret.length());
                }
                
                throw new RuntimeException("Kite API authentication failed: " + errorMessage);
                
            } catch (Exception parseException) {
                logger.error("Failed to parse error response: {}", parseException.getMessage());
                throw new RuntimeException("Kite API authentication failed with HTTP " + e.getStatusCode());
            }
            
        } catch (Exception e) {
            logger.error("Unexpected error during token generation: {}", e.getMessage());
            throw new RuntimeException("Failed to generate access token", e);
        }
    }
    
    /**
     * Store access token securely
     */
    public void storeAccessToken(String accessToken) {
        try {
            // Store in memory
            currentAccessToken.set(accessToken);
            
            // Store in file for persistence
            File configDir = new File("config");
            if (!configDir.exists()) {
                configDir.mkdirs();
            }
            
            try (FileWriter writer = new FileWriter(TOKEN_FILE)) {
                writer.write(accessToken);
            }
            
            logger.info("Access token stored successfully");
            
        } catch (IOException e) {
            logger.error("Failed to store access token: {}", e.getMessage());
            throw new RuntimeException("Failed to store access token", e);
        }
    }
    
    /**
     * Load stored token on startup
     */
    private void loadStoredToken() {
        try {
            if (Files.exists(Paths.get(TOKEN_FILE))) {
                String token = Files.readString(Paths.get(TOKEN_FILE)).trim();
                if (!token.isEmpty()) {
                    currentAccessToken.set(token);
                    logger.info("Loaded stored access token: {}****", token.substring(0, 8));
                }
            }
        } catch (IOException e) {
            logger.warn("Could not load stored access token: {}", e.getMessage());
        }
    }
    
    /**
     * Validate and store manually entered token
     */
    public boolean validateAndStoreToken(String accessToken) {
        try {
            logger.info("Validating access token: {}****", accessToken.substring(0, 8));
            
            // Test the token by making a simple API call
            String response = webClient
                .get()
                .uri("/user/profile")
                .header("Authorization", "token " + apiKey + ":" + accessToken)
                .retrieve()
                .bodyToMono(String.class)
                .block();
            
            JsonNode responseJson = objectMapper.readTree(response);
            
            if (responseJson.has("data")) {
                // Token is valid, store it
                storeAccessToken(accessToken);
                logger.info("Access token validated and stored");
                return true;
            } else {
                logger.warn("Invalid access token provided");
                return false;
            }
            
        } catch (WebClientResponseException e) {
            logger.error("Token validation failed with HTTP {}: {}", e.getStatusCode(), e.getResponseBodyAsString());
            return false;
        } catch (Exception e) {
            logger.error("Error validating access token: {}", e.getMessage());
            return false;
        }
    }
    
    /**
     * Check if currently authenticated
     */
    public boolean isAuthenticated() {
        String token = currentAccessToken.get();
        return token != null && !token.trim().isEmpty() && !token.equals("not_configured");
    }
    
    /**
     * Get current access token
     */
    public String getCurrentAccessToken() {
        return currentAccessToken.get();
    }
    
    /**
     * FIXED: Generate SHA256 hash for checksum (Kite uses SHA256, not MD5)
     */
    private String generateSHA256Hash(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashBytes = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            
            StringBuilder sb = new StringBuilder();
            for (byte b : hashBytes) {
                sb.append(String.format("%02x", b));
            }
            
            return sb.toString();
            
        } catch (Exception e) {
            throw new RuntimeException("Failed to generate SHA256 hash", e);
        }
    }
    
    /**
     * Legacy MD5 method (keeping for reference - DO NOT USE)
     */
    @Deprecated
    private String generateMD5Hash(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] hashBytes = md.digest(input.getBytes(StandardCharsets.UTF_8));
            
            StringBuilder sb = new StringBuilder();
            for (byte b : hashBytes) {
                sb.append(String.format("%02x", b));
            }
            
            return sb.toString();
            
        } catch (Exception e) {
            throw new RuntimeException("Failed to generate MD5 hash", e);
        }
    }
    
    /**
     * Test checksum generation with your actual credentials
     */
    public void debugChecksum(String requestToken) {
        logger.info("=== CHECKSUM DEBUG ===");
        logger.info("API Key: {}", apiKey.substring(0, 4) + "****");
        logger.info("Request Token: {}", requestToken.substring(0, 8) + "****");
        logger.info("API Secret: {}", apiSecret.substring(0, 4) + "****");
        
        String checksumInput = apiKey + requestToken + apiSecret;
        String sha256Checksum = generateSHA256Hash(checksumInput);
        String md5Checksum = generateMD5Hash(checksumInput);
        
        logger.info("SHA256 Checksum: {}", sha256Checksum);
        logger.info("MD5 Checksum: {}", md5Checksum);
        logger.info("Input length: {}", checksumInput.length());
        logger.info("========================");
    }
    
    /**
     * Debug method to verify credentials
     */
    public void debugCredentials() {
        logger.info("=== Kite Credentials Debug ===");
        logger.info("API Key: {}", apiKey != null ? apiKey.substring(0, 4) + "****" : "NULL");
        logger.info("API Secret: {}", apiSecret != null ? apiSecret.substring(0, 4) + "****" : "NULL");
        logger.info("API Key Length: {}", apiKey != null ? apiKey.length() : 0);
        logger.info("API Secret Length: {}", apiSecret != null ? apiSecret.length() : 0);
        logger.info("===============================");
    }
}