import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import jakarta.inject.Inject;

@QuarkusMain
public class ReddisonTestMain implements QuarkusApplication{
    @Inject
    RedisService redisService;

    @Override
    public int run(String... args) throws Exception {
        //redisService.loadTestData();
        redisService.testHgetAllPerformanceSync();
        redisService.testSearchWithIndexPerformanceSync();
        return 0;
    }

    public static void main(String... args) {
        Quarkus.run(ReddisonTestMain.class, args);
    }
    
}
