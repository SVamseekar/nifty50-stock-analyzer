package com.stockanalysis.controller;

import com.stockanalysis.service.KiteAuthService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.HashMap;

@RestController
@RequestMapping("/api/debug")
public class DebugController {
    
    private static final Logger logger = LoggerFactory.getLogger(DebugController.class);
    
    @Autowired
    private KiteAuthService kiteAuthService;
    
    @Value("${kite.api.key}")
    private String apiKey;
    
    @Value("${kite.api.secret}")
    private String apiSecret;
    
    /**
     * Debug current Kite API credentials
     * Visit: http://localhost:8081/stock-analyzer/api/debug/credentials
     */
    @GetMapping("/credentials")
    public ResponseEntity<Map<String, Object>> debugCredentials() {
        Map<String, Object> response = new HashMap<>();
        
        try {
            // Basic credential check
            boolean hasApiKey = apiKey != null && !apiKey.equals("not_configured");
            boolean hasApiSecret = apiSecret != null && !apiSecret.equals("not_configured");
            
            response.put("hasApiKey", hasApiKey);
            response.put("hasApiSecret", hasApiSecret);
            response.put("apiKeyLength", hasApiKey ? apiKey.length() : 0);
            response.put("apiSecretLength", hasApiSecret ? apiSecret.length() : 0);
            response.put("apiKeyPreview", hasApiKey ? apiKey.substring(0, Math.min(4, apiKey.length())) + "****" : "NOT_SET");
            response.put("apiSecretPreview", hasApiSecret ? apiSecret.substring(0, Math.min(4, apiSecret.length())) + "****" : "NOT_SET");
            
            // Expected lengths for Kite API
            response.put("expectedApiKeyLength", 16);
            response.put("expectedApiSecretLength", 32);
            
            // Validation
            Map<String, String> validation = new HashMap<>();
            validation.put("apiKeyLength", hasApiKey && apiKey.length() == 16 ? "CORRECT" : "INCORRECT");
            validation.put("apiSecretLength", hasApiSecret && apiSecret.length() == 32 ? "CORRECT" : "INCORRECT");
            
            response.put("validation", validation);
            response.put("timestamp", java.time.LocalDateTime.now());
            
            // Instructions
            if (!hasApiKey || !hasApiSecret) {
                response.put("instructions", "Set environment variables: KITE_API_KEY and KITE_API_SECRET");
                response.put("getCredentialsFrom", "https://developers.kite.trade/");
            }
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            response.put("error", e.getMessage());
            return ResponseEntity.status(500).body(response);
        }
    }
    
    /**
     * Test checksum generation with a sample request token
     * Visit: http://localhost:8081/stock-analyzer/api/debug/checksum?request_token=YOUR_REQUEST_TOKEN
     */
    @GetMapping("/checksum")
    public ResponseEntity<Map<String, Object>> debugChecksum(
            @RequestParam String request_token) {
        
        Map<String, Object> response = new HashMap<>();
        
        try {
            logger.info("🔍 Testing checksum generation with request_token: {}", request_token.substring(0, 8) + "****");
            
            // Call the debug method
            kiteAuthService.debugChecksum(request_token);
            
            response.put("status", "SUCCESS");
            response.put("message", "Checksum debug info logged. Check your application logs.");
            response.put("requestTokenPreview", request_token.substring(0, 8) + "****");
            response.put("timestamp", java.time.LocalDateTime.now());
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("Error in checksum debug: {}", e.getMessage());
            response.put("error", e.getMessage());
            return ResponseEntity.status(500).body(response);
        }
    }
    
    /**
     * Test the complete authentication flow
     */
    @PostMapping("/test-auth")
    public ResponseEntity<Map<String, Object>> testAuthentication(
            @RequestBody Map<String, String> data) {
        
        Map<String, Object> response = new HashMap<>();
        
        try {
            String requestToken = data.get("request_token");
            
            if (requestToken == null || requestToken.trim().isEmpty()) {
                response.put("error", "request_token is required");
                return ResponseEntity.status(400).body(response);
            }
            
            logger.info("Testing authentication with request_token: {}", requestToken.substring(0, 8) + "****");
            
            // Try to generate access token
            String accessToken = kiteAuthService.generateAccessToken(requestToken);
            
            response.put("status", "SUCCESS");
            response.put("message", "Authentication successful!");
            response.put("accessTokenPreview", accessToken.substring(0, 8) + "****");
            response.put("timestamp", java.time.LocalDateTime.now());
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("Authentication test failed: {}", e.getMessage());
            
            response.put("status", "FAILED");
            response.put("error", e.getMessage());
            response.put("timestamp", java.time.LocalDateTime.now());
            
            return ResponseEntity.status(500).body(response);
        }
    }
}