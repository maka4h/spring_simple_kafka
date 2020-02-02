package com.simpleexample.kafka_prod_cons;

import com.google.gson.Gson;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("api/kafka")
public class KafkaSimpleController {

    private KafkaTemplate<String, String> kafkaTemplate;
    private Gson jsonConverter;

    @Autowired
    public KafkaSimpleController(KafkaTemplate<String, String> kafkaTemplate, Gson jsonConverter) {
        this.kafkaTemplate = kafkaTemplate;
        this.jsonConverter = jsonConverter;
    }

    @PostMapping
    public void post(@RequestBody SimpleModel model) {
        kafkaTemplate.send("myTopic", jsonConverter.toJson(model));
    }

    @PostMapping("/v2")
    public void post(@RequestBody MoreSimpleModel moreSimpleModel) {
        kafkaTemplate.send("myTopic2", jsonConverter.toJson(moreSimpleModel));
    }

    @KafkaListener(topics = "myTopic")
    public void getFromKafka(String model) {
        System.out.println("CONSUMER x myTopic!!! " + model);

        SimpleModel simpleModelRes = (SimpleModel) jsonConverter.fromJson(model, SimpleModel.class);
        System.out.println(simpleModelRes.toString());
    }

    @KafkaListener(topics = "myTopic2")
    public void getFromKafka2(String moreSimpleModel) {
        System.out.println("Consumer x myTopic2 !!" + moreSimpleModel);
        MoreSimpleModel model = jsonConverter.fromJson(moreSimpleModel, MoreSimpleModel.class);
        System.out.println(model.toString());
    }

}
