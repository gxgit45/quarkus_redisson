package com.redistest.service;

import com.google.gson.Gson;
import com.redistest.model.Animal;
import com.redistest.utils.AppUtils;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.redisson.Redisson;
import org.redisson.api.*;
import org.redisson.config.Config;
import java.util.*;
import java.util.concurrent.Future;

@ApplicationScoped
public class RedisService implements AutoCloseable{
    private static final Logger LOG = Logger.getLogger(RedisService.class);

    private static final String SEARCH_KEY = "animals";

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


    public void testPerformance() {
        RList<String> list;
        try{
            long startTime = System.currentTimeMillis();
            list = redisson.getList(SEARCH_KEY);
            if(null == list || list.isEmpty()){
                this.loadTestData();
                list = redisson.getList(SEARCH_KEY);
            }
            Future<List<String>> stringList = list.readAllAsync();
            List<String> data = stringList.get();
            LOG.info(String.format("Operation Took [%s] ms",(System.currentTimeMillis() - startTime)));

        }catch (Exception e){
            LOG.error(e,e);
        }

    }

    public void loadTestData() {
        LOG.info("Loading data.....");
        int maxDataCount = 300000;
        Gson gson = new Gson();
        RList<String> list = redisson.getList(SEARCH_KEY);
        try{

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
                String jsonStr = gson.toJson(animal);
                list.add(jsonStr);
            }

        }catch (Exception e){
            LOG.error(e,e);
        }

        LOG.info(String.format("Loaded [%s] records into the database", maxDataCount));
    }

}
