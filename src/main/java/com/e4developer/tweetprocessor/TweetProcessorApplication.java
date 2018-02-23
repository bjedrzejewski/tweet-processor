package com.e4developer.tweetprocessor;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.messaging.handler.annotation.SendTo;

import java.util.Map;

@EnableBinding(Processor.class)
@SpringBootApplication
public class TweetProcessorApplication {

	public static void main(String[] args) {
		SpringApplication.run(TweetProcessorApplication.class, args);
	}

	@StreamListener(target = Sink.INPUT)
	@SendTo(Processor.OUTPUT)
	public String extractUrls(String tweet) throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		Map<String, Object> tweetMap = mapper.readValue(tweet, new TypeReference<Map<String,Object>>(){});
		return "START-TWEET-TEXT:"+tweetMap.get("text")+":END-TWEET-TEXT";
	}
}
