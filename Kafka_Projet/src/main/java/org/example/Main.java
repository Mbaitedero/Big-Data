package org.example;
import org.example.Comsumer.FirstComsumer;
import org.example.Comsumer.GroupComsumer;
import org.example.Producer.*;

import java.util.concurrent.ExecutionException;

public class Main {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        HelloClass hello= new HelloClass();
        FirstProducer_Demo producerDemo  = new FirstProducer_Demo();
        FirstProducer_Callback producerCallback = new FirstProducer_Callback();
        FirstProducerWithKey producerWithKey = new FirstProducerWithKey();
        FirstComsumer firstComsumer = new FirstComsumer();
        GroupComsumer  groupComsumer = new GroupComsumer();

        // Affichons  le  hello
        hello.saidHello();
        // Pour  le  demo
        //producerDemo.sendMessage();
        //producerCallback.sendMessagesCallback();
       //producerWithKey.sendMessageWithKey();
        //firstComsumer.receiveMessage();
        groupComsumer.receiveByGroupMessage();


    }
}