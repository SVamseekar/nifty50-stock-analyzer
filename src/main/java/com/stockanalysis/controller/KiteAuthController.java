package com.stockanalysis.controller;

import com.stockanalysis.service.KiteAuthService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.view.RedirectView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.HashMap;

@RestController
@RequestMapping("/api/auth")
public class KiteAuthController {
    
    private static final Logger logger = LoggerFactory.getLogger(KiteAuthController.class);
    
    @Autowired
    private KiteAuthService kiteAuthService;
    
    @Value("${kite.api.key}")
    private String apiKey;
    
    /**
     * Step 1: Redirect user to Kite login page
     * Access this URL in browser: http://localhost:8081/stock-analyzer/api/auth/kite/login
     */
    @GetMapping("/kite/login")
    public RedirectView loginWithKite() {
        try {
            String kiteLoginUrl = kiteAuthService.getKiteLoginUrl();
            logger.info("Redirecting to Kite login URL: {}", kiteLoginUrl);
            
            return new RedirectView(kiteLoginUrl);
            
        } catch (Exception e) {
            logger.error("Failed to generate Kite login URL: {}", e.getMessage());
            throw new RuntimeException("Failed to initiate Kite login", e);
        }
    }
    
    /**
     * Step 2: Kite redirects back here with request_token
     * This URL is automatically called by Kite after user logs in
     */
    @GetMapping("/kite/callback")
    public ResponseEntity<Map<String, Object>> handleKiteCallback(
            @RequestParam("request_token") String requestToken,
            @RequestParam(value = "action", required = false) String action,
            @RequestParam(value = "status", required = false) String status) {
        
        Map<String, Object> response = new HashMap<>();
        
        try {
            logger.info("Received Kite callback with request_token: {}", requestToken);
            logger.info("Action: {}, Status: {}", action, status);
            
            if ("login".equals(action) && "success".equals(status)) {
                // Exchange request_token for access_token
                String accessToken = kiteAuthService.generateAccessToken(requestToken);
                
                // Store the access token securely
                kiteAuthService.storeAccessToken(accessToken);
                
                response.put("status", "SUCCESS");
                response.put("message", "Kite authentication successful! You can now use real stock data.");
                response.put("accessToken", accessToken.substring(0, 8) + "...");
                response.put("instructions", "Access token stored. Your stock analysis APIs will now use REAL data from Kite Connect.");
                response.put("nextSteps", "Go back to your stock analyzer and trigger a data fetch!");
                response.put("timestamp", java.time.LocalDateTime.now());
                
                logger.info("Kite authentication successful! Access token generated and stored.");
                
                return ResponseEntity.ok(response);
                
            } else {
                response.put("status", "FAILED");
                response.put("message", "Kite authentication failed or was cancelled");
                response.put("action", action);
                response.put("authStatus", status);
                
                return ResponseEntity.status(400).body(response);
            }
            
        } catch (Exception e) {
            logger.error("Error processing Kite callback: {}", e.getMessage());
            
            response.put("status", "ERROR");
            response.put("message", "Failed to process Kite authentication: " + e.getMessage());
            response.put("timestamp", java.time.LocalDateTime.now());
            
            return ResponseEntity.status(500).body(response);
        }
    }
    
    /**
     * Check current authentication status
     */
    @GetMapping("/kite/status")
    public ResponseEntity<Map<String, Object>> getAuthStatus() {
        Map<String, Object> response = new HashMap<>();
        
        try {
            boolean isAuthenticated = kiteAuthService.isAuthenticated();
            String accessToken = kiteAuthService.getCurrentAccessToken();
            
            response.put("authenticated", isAuthenticated);
            response.put("hasAccessToken", accessToken != null && !accessToken.isEmpty());
            response.put("apiKey", apiKey);
            response.put("timestamp", java.time.LocalDateTime.now());
            
            if (isAuthenticated) {
                response.put("status", "AUTHENTICATED");
                response.put("message", "Kite API is authenticated and ready for REAL data");
                response.put("tokenPreview", accessToken != null ? accessToken.substring(0, 8) + "..." : "null");
                response.put("realDataEnabled", true);
            } else {
                response.put("status", "NOT_AUTHENTICATED");
                response.put("message", "Kite API authentication required for real data");
                response.put("loginUrl", "http://localhost:8081/stock-analyzer/api/auth/kite/login");
                response.put("realDataEnabled", false);
            }
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("Error checking auth status: {}", e.getMessage());
            
            response.put("status", "ERROR");
            response.put("message", "Failed to check authentication status: " + e.getMessage());
            
            return ResponseEntity.status(500).body(response);
        }
    }
    
    /**
     * Manual token entry (alternative to OAuth flow)
     */
    @PostMapping("/kite/token")
    public ResponseEntity<Map<String, Object>> setAccessToken(@RequestBody Map<String, String> tokenData) {
        Map<String, Object> response = new HashMap<>();
        
        try {
            String accessToken = tokenData.get("access_token");
            
            if (accessToken == null || accessToken.trim().isEmpty()) {
                response.put("status", "ERROR");
                response.put("message", "Access token is required");
                return ResponseEntity.status(400).body(response);
            }
            
            // Validate and store the token
            boolean isValid = kiteAuthService.validateAndStoreToken(accessToken.trim());
            
            if (isValid) {
                response.put("status", "SUCCESS");
                response.put("message", "Access token stored successfully - REAL data enabled!");
                response.put("authenticated", true);
                response.put("realDataEnabled", true);
            } else {
                response.put("status", "FAILED");
                response.put("message", "Invalid access token");
                response.put("authenticated", false);
                response.put("realDataEnabled", false);
            }
            
            response.put("timestamp", java.time.LocalDateTime.now());
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("Error setting access token: {}", e.getMessage());
            
            response.put("status", "ERROR");
            response.put("message", "Failed to set access token: " + e.getMessage());
            
            return ResponseEntity.status(500).body(response);
        }
    }
    //Debug endpoint to check credentials
    @GetMapping("/kite/debug")
    public ResponseEntity<Map<String, Object>> debugCredentials() {
        Map<String, Object> response = new HashMap<>();
            
        kiteAuthService.debugCredentials();
            
        response.put("apiKeyLength", apiKey != null ? apiKey.length() : 0);
        response.put("apiKeyPreview", apiKey != null ? apiKey.substring(0, 4) + "****" : "NULL");
        response.put("timestamp", java.time.LocalDateTime.now());
            
        return ResponseEntity.ok(response);
    }
}