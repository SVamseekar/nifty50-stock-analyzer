package com.stockanalysis.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.ExchangeStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import jakarta.annotation.PostConstruct;

@Configuration
public class KiteApiConfig {
    
    private static final Logger logger = LoggerFactory.getLogger(KiteApiConfig.class);
    
    @Value("${kite.api.key:not_configured}")
    private String apiKey;
    
    @Value("${kite.api.secret:not_configured}")
    private String apiSecret;
    
    @Value("${kite.access.token:not_configured}")
    private String accessToken;
    
    @Value("${kite.api.base.url:https://api.kite.trade}")
    private String baseUrl;
    
    @Value("${kite.api.rate.limit.delay.ms:400}")
    private int rateLimitDelay;
    
    @Value("${kite.api.retry.max.attempts:3}")
    private int maxRetryAttempts;
    
    @PostConstruct
    public void validateConfiguration() {
        logger.info("=== Kite API Configuration Validation ===");
        
        boolean apiKeyConfigured = isCredentialConfigured(apiKey);
        boolean secretConfigured = isCredentialConfigured(apiSecret);
        boolean tokenConfigured = isCredentialConfigured(accessToken);
        
        logger.info("API Key configured: {}", apiKeyConfigured ? "YES" : "NO");
        logger.info("API Secret configured: {}", secretConfigured ? "YES" : "NO");
        logger.info("Access Token configured: {}", tokenConfigured ? "YES" : "NO");
        logger.info("Base URL: {}", baseUrl);
        logger.info("Rate limit delay: {}ms", rateLimitDelay);
        
        if (isFullyConfigured()) {
            logger.info("Kite API is FULLY CONFIGURED - Real API calls enabled");
        } else {
            logger.warn("Kite API credentials incomplete - Please authenticate via OAuth");
            logger.warn("Visit: http://localhost:8081/stock-analyzer/api/auth/kite/login");
        }
        
        logger.info("==========================================");
    }
    
    @Bean
    public WebClient kiteWebClient() {
        // Configure larger buffer for CSV responses from instruments API
        ExchangeStrategies strategies = ExchangeStrategies.builder()
            .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(16 * 1024 * 1024))
            .build();
        
        return WebClient.builder()
            .baseUrl(baseUrl)
            .defaultHeader("X-Kite-Version", "3")
            .defaultHeader("User-Agent", "StockAnalyzer/2.0")
            .exchangeStrategies(strategies)
            .build();
    }
    
    @Bean
    public KiteCredentials kiteCredentials() {
        return new KiteCredentials(apiKey, apiSecret, accessToken, 
                                 rateLimitDelay, maxRetryAttempts, isFullyConfigured());
    }
    
    // For backward compatibility with existing code
    @Bean
    public ApiConfig apiConfig() {
        return new ApiConfig(apiKey, apiSecret, accessToken, 
                           rateLimitDelay, maxRetryAttempts, isFullyConfigured());
    }
    
    private boolean isCredentialConfigured(String credential) {
        return credential != null && 
               !credential.trim().isEmpty() && 
               !credential.equals("not_configured") &&
               !credential.startsWith("YOUR_ACTUAL_");
    }
    
    private boolean isFullyConfigured() {
        return isCredentialConfigured(apiKey) && 
               isCredentialConfigured(apiSecret);
        // Note: accessToken is obtained via OAuth, so not required at startup
    }
    
    // KiteCredentials class for the new architecture
    public static class KiteCredentials {
        private final String apiKey;
        private final String apiSecret;
        private final String accessToken;
        private final int rateLimitDelay;
        private final int maxRetryAttempts;
        private final boolean configured;
        
        public KiteCredentials(String apiKey, String apiSecret, String accessToken, 
                             int rateLimitDelay, int maxRetryAttempts, boolean configured) {
            this.apiKey = apiKey;
            this.apiSecret = apiSecret;
            this.accessToken = accessToken;
            this.rateLimitDelay = rateLimitDelay;
            this.maxRetryAttempts = maxRetryAttempts;
            this.configured = configured;
        }
        
        // Getters (no setters for security)
        public String getApiKey() { return apiKey; }
        public String getApiSecret() { return apiSecret; }
        public String getAccessToken() { return accessToken; }
        public int getRateLimitDelay() { return rateLimitDelay; }
        public int getMaxRetryAttempts() { return maxRetryAttempts; }
        public boolean isConfigured() { return configured; }
        
        public String getAuthorizationHeader() {
            // This method is not used anymore - KiteAuthService provides the token
            throw new UnsupportedOperationException("Use KiteAuthService.getCurrentAccessToken() instead");
        }
        
        @Override
        public String toString() {
            return String.format("KiteCredentials{configured=%s, rateLimitDelay=%d, maxRetryAttempts=%d}", 
                               configured, rateLimitDelay, maxRetryAttempts);
        }
    }
    
    // ApiConfig class for backward compatibility
    public static class ApiConfig {
        private final String apiKey;
        private final String apiSecret;
        private final String accessToken;
        private final int rateLimitDelay;
        private final int maxRetryAttempts;
        private final boolean configured;
        
        public ApiConfig(String apiKey, String apiSecret, String accessToken, 
                        int rateLimitDelay, int maxRetryAttempts, boolean configured) {
            this.apiKey = apiKey;
            this.apiSecret = apiSecret;
            this.accessToken = accessToken;
            this.rateLimitDelay = rateLimitDelay;
            this.maxRetryAttempts = maxRetryAttempts;
            this.configured = configured;
        }
        
        public String getApiKey() { return apiKey; }
        public String getApiSecret() { return apiSecret; }
        public String getAccessToken() { return accessToken; }
        public int getRateLimitDelay() { return rateLimitDelay; }
        public int getMaxRetryAttempts() { return maxRetryAttempts; }
        public boolean isConfigured() { return configured; }
        
        public String getAuthorizationHeader() {
            throw new UnsupportedOperationException("Use KiteAuthService.getCurrentAccessToken() instead");
        }
        
        @Override
        public String toString() {
            return String.format("ApiConfig{configured=%s, rateLimitDelay=%d, maxRetryAttempts=%d}", 
                               configured, rateLimitDelay, maxRetryAttempts);
        }
    }
}