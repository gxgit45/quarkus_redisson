package com.redistest;

import com.google.gson.Gson;
import com.redistest.model.Animal;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.redisson.Redisson;
import org.redisson.api.*;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;
import java.util.*;

@ApplicationScoped
public class RedisService implements AutoCloseable{
    private static final Logger LOG = Logger.getLogger(RedisService.class);

    private static final String SEARCH_KEY = "animal:dog:";

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
        try{
            RMap<String, String> map = redisson.getMap(SEARCH_KEY,StringCodec.INSTANCE);
            if(map == null || map.isEmpty()){
                this.loadTestData();
            }
            long startTime = System.currentTimeMillis();
            map = redisson.getMap(SEARCH_KEY,StringCodec.INSTANCE);
            Map<String,String> data = map.readAllMap();
            LOG.info(String.format("HgetAll Took [%s] ms",(System.currentTimeMillis() - startTime)));

        }catch (Exception e){
            LOG.error(e,e);
        }

    }

    public void loadTestData() {
        LOG.info("Loading data.....");
        int maxDataCount = 300000;
        Gson gson = new Gson();

        // second one for batch query
        for(int i = 0; i < maxDataCount; i++ ){
            RMap<String, Object> map = redisson.getMap(SEARCH_KEY, StringCodec.INSTANCE);
            Animal animal = new Animal();
            animal.setId(i);
            animal.setHabitat("Jungle");
            animal.setColor("Brown");
            animal.setName("George");
            animal.setAType("Gorilla");
            animal.setShape("Human Shape");
            animal.setFavoriteFood("Banannas");
            animal.setCategoryId(i+i);
            map.put("value:" + i,gson.toJson(animal));
        }

        LOG.info(String.format("Loaded [%s] records into the database", maxDataCount));
    }

}
