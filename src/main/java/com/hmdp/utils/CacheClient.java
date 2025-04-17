package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

@Slf4j
@Component
public class CacheClient {
    private final StringRedisTemplate stringRedisTemplate;

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void set(String key, Object value, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit) {
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));

        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    public <R, ID> R queryWithPassThrough(
            Long time, TimeUnit unit, String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback
    ) {
        //1. search the shop data from redis
        String json = stringRedisTemplate.opsForValue().get(keyPrefix + id);
        //2. exist or not
        if (StrUtil.isNotBlank(json)) {
            //3. exist => return data
            return JSONUtil.toBean(json, type);
        }
        //determine if the value is null or not(avoid the Cache Penetration)
        if (json != null) {
            return null;
        }
        //4. not exist => search the data from the database
        R r = dbFallback.apply(id);
        //5. not exist => return error
        if (r == null) {
            stringRedisTemplate.opsForValue().set(keyPrefix + id,"",CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        //6. exist => store in the redis
        this.set(keyPrefix + id, JSONUtil.toJsonStr(r), time, unit);

        return r;
    }

    public <R, ID> R queryWithLogicalExpire(
            Long time, TimeUnit unit, String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback
    ) {
        //1. search the shop data from redis
        String json = stringRedisTemplate.opsForValue().get(keyPrefix + id);
        //2. exist or not
        if (StrUtil.isBlank(json)) {
            //3. not exist => return null
            return null;
        }
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        JSONObject data = (JSONObject) redisData.getData();
        R r = JSONUtil.toBean(data, type);
        LocalDateTime expireTime = redisData.getExpireTime();
        // judge if cache is expired
        if (expireTime.isAfter(LocalDateTime.now())) {
            // if not expired => return shop data
            return r;
        }
        // if expired => try to get lock
        String lockKey = LOCK_SHOP_KEY + id;
        if (tryLock(lockKey)){
            //if : can get lock => create new thread(using thread pool) to do the update process
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                //update cache
                try {
                    //查数据库
                    R r1 = dbFallback.apply(id);
                    //update Redis
                    this.setWithLogicalExpire(keyPrefix + id, r1, time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    unLock(lockKey);
                }
            });
        }
        //if: can not get lock => return shop data(old)

        return r;
    }
    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", LOCK_SHOP_TTL, TimeUnit.MINUTES);
        return BooleanUtil.isTrue(flag);
    }

    private boolean unLock(String key) {
        Boolean flag = stringRedisTemplate.delete(key);
        return BooleanUtil.isTrue(flag);
    }

}
