package com.redistest.service;


import com.google.gson.Gson;
import com.redistest.model.Animal;
import com.redistest.utils.AppUtils;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.redisson.Redisson;
import org.redisson.api.*;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


@ApplicationScoped
public class RedisService implements AutoCloseable{
    private static final Logger LOG = Logger.getLogger(RedisService.class);

    private final Gson gson = new Gson();

    @ConfigProperty(name = "redisson.search.key")
    private String searchKey;

    private final RedissonClient redisson;
    public RedisService(@ConfigProperty(name = "redisson.timeout") int redissonTimeout
            ,@ConfigProperty(name = "redisson.url") String redissonUrl){
        Config config = new Config();
        config.useSingleServer().setAddress(redissonUrl).setTimeout(redissonTimeout);
        redisson = Redisson.create(config);
    }

    @Override
    public void close()  {
        try{
            if(redisson != null){
                redisson.shutdown();
                LOG.info("Redisson connections closed!");
            }

        }catch (Exception e){
            LOG.error(e,e);
        }
    }


    public void testHgetAllPerformance() {
            RMap<String, String> map = redisson.getMap(this.searchKey,StringCodec.INSTANCE);
            if(map == null || map.isEmpty()){
                this.loadTestData();
            }
            long startTime = System.currentTimeMillis();
            map = redisson.getMap(this.searchKey,StringCodec.INSTANCE);
            Map<String,String> dataMap = map.readAllMap();
            final Map<String, Animal> animalMap = new ConcurrentHashMap<>();
                dataMap.entrySet().parallelStream().filter(Objects::nonNull).forEach(item ->{
                    try {
                        animalMap.put(item.getKey()
                                ,gson.fromJson(AppUtils.uncompressString(item.getValue()),Animal.class));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                });
            LOG.info(String.format("Time for HGETALL is [%s] ms",(System.currentTimeMillis() - startTime)));
    }

    public void loadTestData()  {
        LOG.info("Loading data.....");
        int maxDataCount = 200000;
        // second one for batch query
        for(int i = 0; i < maxDataCount; i++ ){
            RMap<String, Object> map = redisson.getMap(this.searchKey, StringCodec.INSTANCE);
            Animal animal = new Animal();
            animal.setId(i);
            animal.setHabitat("Jungle");
            animal.setColor("Brown");
            animal.setName("George");
            animal.setAType("Gorilla");
            animal.setShape("Human Shape");
            animal.setFavoriteFood("Bananas");
            animal.setCategoryId(i+i);
            String jsonString = gson.toJson(animal);
            try{
                map.put("value:" + i, AppUtils.compressString(jsonString));
            }catch (Exception e){
                LOG.error(e,e);
            }

        }

        LOG.info(String.format("Loaded [%s] records into the database", maxDataCount));
    }

}
