package com.stockanalysis.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
public class WebController {

    @GetMapping("/test")
    @ResponseBody
    public String test() {
        return "WebController is working!";
    }
    
    @GetMapping({"/", "/dashboard"})
    public String dashboard() {
        return "stock-dashboard";
    }
    
    @GetMapping("/stock-analysis")
    public String analysis() {
        return "stock-analysis";
    }
}