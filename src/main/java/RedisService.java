
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;
import org.redisson.Redisson;
import org.redisson.api.*;
import org.redisson.api.search.query.QueryFilter;
import org.redisson.api.search.query.QueryOptions;
import org.redisson.api.search.query.SearchResult;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@ApplicationScoped
public class RedisService implements AutoCloseable{
    private static final Logger LOG = Logger.getLogger(RedisService.class);

    private final long RECORD_SEARCH_CNT = 15000L;

    private RedissonClient redisson = null;
    public RedisService(){
        Config config = new Config();
        config.useSingleServer().setAddress("redis://127.0.0.1:6379").setTimeout(120000);
        redisson = Redisson.create(config);
    }

    @Override
    public void close() throws Exception {
        try{
            if(redisson != null){
                redisson.shutdown();
                LOG.info("Redisson connections closed!");
            }

        }catch (Exception e){
            LOG.error(e,e);
        }
    }

    public void printFoo() {
        System.out.println("Foo");
    }

    public void testHgetAllPerformanceSync() {
        final Set<String> keySet = new HashSet<>();

        try{

         keySet.addAll(getKeys());

          if(keySet.size() == 0){
              loadTestData();
              keySet.addAll(getKeys());
          }

          int keySetSize = keySet != null && !keySet.isEmpty() ? keySet.size():0;
          LOG.info(String.format("Number of keys is [%s]",keySetSize));


          long startTime = System.currentTimeMillis();

          RBatch batch = redisson.createBatch(BatchOptions.defaults());
          keySet.parallelStream().limit(this.RECORD_SEARCH_CNT).forEach(key ->{
              batch.getMap(key,StringCodec.INSTANCE).readAllEntrySetAsync();

          });

          RFuture<BatchResult<?>> resFuture = batch.executeAsync();
          BatchResult<?> data = resFuture.get();


          LOG.info(String.format("HgetAll Took [%s] ms",(System.currentTimeMillis() - startTime)));

        }catch (Exception e){
            LOG.error(e,e);
        }

    }

    public void testSearchWithIndexPerformanceSync() {
        final Set<String> keySet = new HashSet<>();

        try{

            keySet.addAll(getKeys());

            if(keySet.size() == 0){
                loadTestData();
                keySet.addAll(getKeys());
            }

            int keySetSize = keySet != null && !keySet.isEmpty() ? keySet.size():0;
            LOG.info(String.format("Number of keys is [%s]",keySetSize));

            long startTime = System.currentTimeMillis();

            AtomicInteger min = new AtomicInteger();
            AtomicInteger max = new AtomicInteger();

            RSearch s = redisson.getSearch(StringCodec.INSTANCE);
            QueryFilter queryFilter = QueryFilter.numeric("id").min(min.doubleValue()).max(max.doubleValue());
            List<SearchResult> searchResults = Collections.synchronizedList(new ArrayList<>());

            keySet.parallelStream().limit(this.RECORD_SEARCH_CNT).forEach(key -> {
                int id = Integer.parseInt(key.split(":")[2]);
                searchResults.add(s.search("idx:animal", "*", QueryOptions.defaults().filters(queryFilter).limit(id,id)));

            });

            LOG.info(String.format("Search Took [%s] ms",(System.currentTimeMillis() - startTime)));

        }catch (Exception e){
            LOG.error(e,e);
        }

    }

    private Set<String> getKeys() {
        RKeys keys =  redisson.getKeys();
        return keys.getKeysStream().collect(Collectors.toSet());
    }

    public void loadTestData() {
        LOG.info("Loading data.....");
        int maxDataCount = 300000;
        for(int i = 0; i < maxDataCount; i++ ){
            String iValue = String.valueOf(i);
            RMap<String, Object> map = redisson.getMap("animal:dog:" + iValue, StringCodec.INSTANCE);
            map.put("id", i);
            map.put("type", "dog");
            map.put("color", "blue");
            map.put("sex","female");
            map.put("numberOfLegs",2);
            map.put("numberOfArms",3);
        }

        LOG.info(String.format("Loaded [%s] records into the database", maxDataCount));
    }


}
