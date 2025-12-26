package org.example.Producer;

import org.slf4j.Logger  ;
import  org.slf4j.LoggerFactory  ;

public class HelloClass {
    public void saidHello() {
        Logger logger  = LoggerFactory.getLogger(HelloClass.class)  ;
        logger.info(" This is how you configure Java Logging with SLF4J");
    }

}
