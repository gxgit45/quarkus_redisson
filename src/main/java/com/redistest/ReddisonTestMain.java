package com.redistest;

import com.redistest.service.RedisGetService;
import com.redistest.service.RedisHgetService;
import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import jakarta.inject.Inject;

@QuarkusMain
public class ReddisonTestMain implements QuarkusApplication{
    @Inject
    RedisHgetService redisService;

    @Inject
    RedisGetService redisGetService;

    @Override
    public int run(String... args) throws Exception {
        //redisService.testHgetAllPerformance();
        redisGetService.testPerformance();;
        return 0;
    }
    public static void main(String... args) {
        Quarkus.run(ReddisonTestMain.class, args);
    }
    
}
