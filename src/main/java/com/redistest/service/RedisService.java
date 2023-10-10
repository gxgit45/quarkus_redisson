package com.redistest.service;

import com.google.gson.Gson;
import com.redistest.model.Animal;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.redisson.Redisson;
import org.redisson.api.*;
import org.redisson.config.Config;
import java.time.Duration;
import java.util.*;

@ApplicationScoped
public class RedisService implements AutoCloseable{
    private static final Logger LOG = Logger.getLogger(RedisService.class);

    private Set<String> listKeys;

    final Gson gson = new Gson();


    private final RedissonClient redisson;
    public RedisService(@ConfigProperty(name = "redisson.timeout") int redissonTimeout
            ,@ConfigProperty(name = "redisson.url") String redissonUrl){
        Config config = new Config();
        config.useSingleServer().setAddress(redissonUrl).setTimeout(redissonTimeout);
        redisson = Redisson.create(config);

        // set list keys
        listKeys = new HashSet<>();
        listKeys.add("Dog");
        listKeys.add("Cow");
        listKeys.add("Gorilla");
        listKeys.add("Goat");
        listKeys.add("Ox");
        listKeys.add("Fox");
        listKeys.add("Eagle");
        listKeys.add("Alligator");
        listKeys.add("Crow");
        listKeys.add("Snake");

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
      final List<String> syncList = Collections.synchronizedList(new ArrayList<>());
        try{
            this.loadTestData();
            long startTime = System.currentTimeMillis();
            listKeys.parallelStream().filter(Objects::nonNull).forEach(key ->{
                RList<String> list = this.redisson.getList(key);
                if(null != list && !list.isEmpty()){
                    syncList.addAll(list.readAll());
                }
            });

            LOG.info(String.format("Pull [%s] Animals from cache in [%s] ms",syncList.size()
                    ,(System.currentTimeMillis() - startTime)));

        }catch (Exception e){
            LOG.error(e,e);
        }

    }

    public void loadTestData() {
        int maxDataCount = 66000;

        try{
            listKeys.stream().forEach(listName-> {
                RList<String> list = redisson.getList(listName);
                if(list == null || list.isEmpty() || list.size() != maxDataCount){

                    if(null != list)
                        list.clear();

                    LOG.info("Loading data for Animal " + listName);
                    List<String> animalList = new ArrayList<>();
                    for(int i = 0; i < maxDataCount; i++ ){
                        Animal animal = new Animal();
                        animal.setId(i);
                        animal.setHabitat("Random");
                        animal.setColor("Brown");
                        animal.setName(listName + " George");
                        animal.setAType(listName);
                        animal.setShape(listName + " Shape");
                        animal.setFavoriteFood("Food");
                        animal.setCategoryId(i+i);
                        String jsonStr = gson.toJson(animal);
                        animalList.add(jsonStr);

                    }
                    //list.expireIfNotSet(Duration.ofHours(1));
                    list.addAll(animalList);


                    LOG.info("Completed loading data for Animal " + listName);

                }

            });

        }catch (Exception e){
            LOG.error(e,e);
        }

    }

}
