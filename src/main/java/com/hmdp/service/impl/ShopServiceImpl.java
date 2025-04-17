package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;

    @Override
    public Result queryById(Long id) {
        //1.缓存穿透
         Shop shop = cacheClient.queryWithPassThrough(CACHE_SHOP_TTL,TimeUnit.MINUTES, CACHE_SHOP_KEY, id,Shop.class,this::getById);
        //互斥锁解决缓存击穿
//        Shop shop = queryWithMutex(id);
        //用逻辑过期解决缓存击穿
        //Shop shop = queryWithLogicalExpire(id);
        if (shop == null) {
            return Result.fail("This shot is not found");
        }
        return Result.ok(shop);
    }
/*
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public Shop queryWithLogicalExpire(Long id) {
        //1. search the shop data from redis
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
        //2. exist or not
        if (StrUtil.isBlank(shopJson)) {
            //3. not exist => return null
            return null;
        }
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        JSONObject data = (JSONObject) redisData.getData();
        Shop shop = JSONUtil.toBean(data, Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        // judge if cache is expired
        if (expireTime.isAfter(LocalDateTime.now())) {
            // if not expired => return shop data
            return shop;
        }
        // if expired => try to get lock
        String lockKey = LOCK_SHOP_KEY + id;
        if (tryLock(lockKey)){
            //if : can get lock => create new thread(using thread pool) to do the update process
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                //update cache
                try {
                    this.saveShop2Redis(id, 20L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    unLock(lockKey);
                }
            });
        }
        //if: can not get lock => return shop data(old)

        return shop;
    }

    public Shop queryWithMutex(Long id) {
        //1. search the shop data from redis
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
        //2. exist or not
        if (StrUtil.isNotBlank(shopJson)) {
            //3. exist => return data
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        //determine if the value is null or not(avoid the Cache Penetration)
        if (shopJson != null) {
            return null;
        }
        //Implement the Mutex function(get lock and del lock)
        // Get the lock
        String lockKey = LOCK_SHOP_KEY + id;
        Shop shop = null;
        try {
            boolean isLock = tryLock(lockKey);
            // Determine if getting lock successfully
            //can not get lock => wait
            if (!isLock) {
                Thread.sleep(50);
                return queryWithMutex(id);
            }
            // get lock => do the action: 1.get data from dataset 2.transmission data to Redis 3. return data
            //4. not exist => search the data from the database
            shop = getById(id);
            //5. not exist => return error
            if (shop == null) {
                stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id,"",CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            //6. exist => store in the redis
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally {
            // release lock
            unLock(lockKey);
        }

        return shop;
    }

    public Shop queryWithPassThrough(Long id) {
        //1. search the shop data from redis
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
        //2. exist or not
        if (StrUtil.isNotBlank(shopJson)) {
            //3. exist => return data
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        //determine if the value is null or not(avoid the Cache Penetration)
        if (shopJson != null) {
            return null;
        }
        //4. not exist => search the data from the database
        Shop shop = getById(id);
        //5. not exist => return error
        if (shop == null) {
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id,"",CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        //6. exist => store in the redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);

        return shop;
    }

    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", LOCK_SHOP_TTL, TimeUnit.MINUTES);
        return BooleanUtil.isTrue(flag);
    }

    private boolean unLock(String key) {
        Boolean flag = stringRedisTemplate.delete(key);
        return BooleanUtil.isTrue(flag);
    }

    private void saveShop2Redis(Long id, Long expireSeconds) {
        //1.查询店铺数据
        Shop shop = getById(id);
        //2.封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        //3.写入Redis（无TTL）
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }
*/
    @Override
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("shop id is null");
        }
        //1. update database
        updateById(shop);
        //2. delete cache
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);

        return Result.ok();
    }
}
