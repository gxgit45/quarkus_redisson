package com.redistest.service;

import com.google.gson.Gson;
import com.redistest.model.Animal;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.redisson.Redisson;
import org.redisson.api.*;
import org.redisson.client.codec.StringCodec;
import org.redisson.codec.LZ4Codec;
import org.redisson.config.Config;

import java.time.Duration;
import java.util.*;

@ApplicationScoped
public class RedisGetService implements AutoCloseable{
    private static final Logger LOG = Logger.getLogger(RedisGetService.class);

    private int maxDataCount = 150000;

    private final Gson gson = new Gson();


    private final RedissonClient redisson;
    public RedisGetService(@ConfigProperty(name = "redisson.timeout") int redissonTimeout
            , @ConfigProperty(name = "redisson.url") String redissonUrl){

        Config config = new Config();
        config.setCodec(new LZ4Codec());
        config.useSingleServer()
                .setAddress(redissonUrl).setTimeout(redissonTimeout).setConnectionMinimumIdleSize(24);
        redisson = Redisson.create(config);
    }

    @Override
    public void close()  {
        try{
            if(redisson != null){
                redisson.shutdown();
            }

        }catch (Exception e){
            LOG.error(e,e);
        }
    }

    public void testPerformance() {
        try{
            Set<String> keys = this.getKeys();
            if(!this.redisson.getBucket("1").isExists())
                this.loadTestData(keys);;
            List<String> dataList = Collections.synchronizedList(new ArrayList<>());
            long startTime = System.currentTimeMillis();

            RBatch batch = this.redisson.createBatch(BatchOptions.defaults());

            keys.parallelStream().forEach(key->{
                try{

                    batch.getBucket(key).getAsync();

                }catch (Exception e){
                    LOG.error(e,e);
                }
            });


            try{
                BatchResult<?> data = batch.execute();
                dataList.addAll((List<String>)data.getResponses());

            }catch (Exception e){
                LOG.error(e,e);
            }

            LOG.info(String.format("Pulled [%s] records in [%s] ms",dataList.size()
                    ,(System.currentTimeMillis() - startTime)));

        }catch (Exception e){
            LOG.error(e,e);
        }

    }

    private Set<String> getKeys() {
        String prefix = "value:";
        List<String> keys = new ArrayList<>();
        Set<String> keySet = new HashSet<>();
        for(int i = 0 ;i< maxDataCount;i++ ){
            keys.add(String.valueOf(i));
        }
        keySet.addAll(keys);
        return keySet;
    }

    public void loadTestData(Set<String> keys) {
        LOG.info("Loading data.....");
        keys.stream().forEach(key->{
            try{
                RBucket<String> stringRBucket = redisson.getBucket(key);
                Animal animal = new Animal();
                animal.setHabitat("Jungle");
                animal.setColor("Brown");
                animal.setName("George");
                animal.setAType("Gorilla");
                animal.setShape("Human Shape");
                animal.setFavoriteFood("Bananas");
                stringRBucket.set(gson.toJson(animal));
                stringRBucket.expire(Duration.ofMinutes(10));

            }catch (Exception e){
                LOG.error(e,e);
            }
        });

        LOG.info(String.format("Loaded [%s] records into the database", maxDataCount));
    }
}