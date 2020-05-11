package com.redislabs.university.RU102J.dao;

import com.redislabs.university.RU102J.RedisConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import java.time.ZonedDateTime;
import java.util.UUID;

import com.redislabs.university.RU102J.core.KeyHelper;

public class RateLimiterSlidingDaoRedisImpl implements RateLimiter {

    private final JedisPool jedisPool;
    private final long windowSizeMS;
    private final long maxHits;

    public RateLimiterSlidingDaoRedisImpl(JedisPool pool, long windowSizeMS,
                                          long maxHits) {
        this.jedisPool = pool;
        this.windowSizeMS = windowSizeMS;
        this.maxHits = maxHits;
    }

    // Challenge #7
    @Override
    public void hit(String name) throws RateLimitExceededException {
        // START CHALLENGE #7
        String key = RedisSchema.getRateLimiterKey(name, (int) windowSizeMS, maxHits);
        try(Jedis jedis = jedisPool.getResource()){
            try( Transaction transaction = jedis.multi() ){
                long millis = System.currentTimeMillis();
                transaction.zadd(key, millis, UUID.randomUUID().toString() );
                transaction.zremrangeByScore(key,     0     , millis - windowSizeMS  );
                Response<Long> countedHits = transaction.zcard(key);
                transaction.exec();
                if(countedHits.get() > maxHits ){
                    throw new RateLimitExceededException();
                }
            }
        }
        
        // END CHALLENGE #7
    }
}
