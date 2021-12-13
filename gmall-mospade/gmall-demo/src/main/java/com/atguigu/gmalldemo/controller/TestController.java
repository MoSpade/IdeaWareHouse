package com.atguigu.gmalldemo.controller;


import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TestController {

    @RequestMapping("test1")
    public String test1(){
        System.out.println("123");
        return "success";
    }
    @RequestMapping("test2")
    public String test2(
            @RequestParam("name") String name,
            @RequestParam("age") int age){
        System.out.println("1234");
        return "name:"+ name +"age:"+age;
    }
}
