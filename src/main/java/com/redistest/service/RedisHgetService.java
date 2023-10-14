package com.redistest.service;

import com.google.gson.Gson;
import com.redistest.model.Animal;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.redisson.Redisson;
import org.redisson.api.*;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;

import java.time.Duration;
import java.util.*;

@ApplicationScoped
public class RedisHgetService implements AutoCloseable{
    private static final Logger LOG = Logger.getLogger(RedisHgetService.class);

    private static final String SEARCH_KEY = "animal";

    private int maxDataCount = 1000000;

    private final RedissonClient redisson;
    public RedisHgetService(@ConfigProperty(name = "redisson.timeout") int redissonTimeout
            , @ConfigProperty(name = "redisson.url") String redissonUrl){
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
        try{
            RMap<String, String> map = redisson.getMap(SEARCH_KEY,StringCodec.INSTANCE);
            if(map == null || map.isEmpty() || map.size() != this.maxDataCount){
                this.loadTestData();
            }
            Set<String> keys = this.getKeys();
            List<String> dataList = Collections.synchronizedList(new ArrayList<>());
            long startTime = System.currentTimeMillis();

            LOG.info(String.format("Hget pulled [%s] records in [%s] ms",dataList.size()
                    ,(System.currentTimeMillis() - startTime)));

        }catch (Exception e){
            LOG.error(e,e);
        }

    }

    private Set<String> getKeys() {
        int maxSize = 120000;
        String prefix = "value:";
        List<String> keys = new ArrayList<>();
        Set<String> keySet = new HashSet<>();
        for(int i = 0 ;i< maxSize;i++ ){
            keys.add(prefix + i);
        }
        keySet.addAll(keys);
        return keySet;
    }

    public void loadTestData() {
        LOG.info("Loading data.....");

        Gson gson = new Gson();
        RMap<String, Object> map = redisson.getMap(SEARCH_KEY, StringCodec.INSTANCE);
        map.clear();

        // second one for batch query
        for(int i = 0; i < maxDataCount; i++ ){
            Animal animal = new Animal();
            animal.setId(i);
            animal.setHabitat("Jungle");
            animal.setColor("Brown");
            animal.setName("George");
            animal.setAType("Gorilla");
            animal.setShape("Human Shape");
            animal.setFavoriteFood("Bananas");
            animal.setCategoryId(i+i);
            map.put("value:" + i,gson.toJson(animal));

        }
        map.expire(Duration.ofMinutes(5));

        LOG.info(String.format("Loaded [%s] records into the database", maxDataCount));
    }

}
