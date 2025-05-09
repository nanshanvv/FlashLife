package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import jodd.util.Consumers;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    StringRedisTemplate stringRedisTemplate;
    @Resource
    RedissonClient redissonClient;

    private static final DefaultRedisScript<Long> SECKIll_SCRIPT;
    static {
        SECKIll_SCRIPT = new DefaultRedisScript<>();
        SECKIll_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKIll_SCRIPT.setResultType(Long.class);
    }
    //线程池创建
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();
    private volatile boolean running = true;

    @PostConstruct
    private void init() {
        String queueName = "stream.orders";
        String groupName = "g1";

        try {
            stringRedisTemplate.opsForStream().createGroup(queueName, ReadOffset.from("0"), groupName);
            log.info("Redis Stream消费组创建成功：{}", groupName);
        } catch (Exception e) {
            log.info("Redis Stream消费组可能已经存在，无需创建。");
        }

        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }


    @PreDestroy
    public void shutdown() {
        running = false;
        SECKILL_ORDER_EXECUTOR.shutdown();
        try {
            if (!SECKILL_ORDER_EXECUTOR.awaitTermination(10, TimeUnit.SECONDS)) {
                SECKILL_ORDER_EXECUTOR.shutdownNow();
            }
        } catch (InterruptedException e) {
            SECKILL_ORDER_EXECUTOR.shutdownNow();
        }
    }

    private class VoucherOrderHandler implements Runnable {
        private String queueName = "stream.orders";
        @Override
        public void run() {
            while (true) {
                try {
                    //获取消息队列中的订单信息
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    //判断消息是否获取成功
                    if(list == null || list.isEmpty()) {
                        //失败：没有消息则继续下一次循环
                        continue;
                    }
                    //解析消息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    //成功：创建订单
                    handleVoucherOrder(voucherOrder);
                    //ACK确认
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e) {
                    log.error("订单异常", e);
                    handlePendingList();
                }

            }
        }

        private void handlePendingList() {
            while (true) {
                try {
                    //获取PendingList中的订单信息
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );
                    //判断消息是否获取成功
                    if(list == null || list.isEmpty()) {
                        //失败：PendingList没有消息则结束
                        break;
                    }
                    //解析消息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    //成功：创建订单
                    handleVoucherOrder(voucherOrder);
                    //ACK确认
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e) {
                    log.error("订单异常", e);
                }

            }
        }
    }

    //线程任务
    //为了让这个class一初始化就开始执行任务，用注解：PostConstruct
//    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024*1024);
//    private class VoucherOrderHandler implements Runnable {
//        @Override
//        public void run() {
//            while (true) {
//                try {
//                    //获取队列中的订单信息
//                    VoucherOrder voucherOder = orderTasks.take();
//                    //创建订单
//                    handleVoucherOrder(voucherOder);
//                } catch (Exception e) {
//                    log.error("订单异常", e);
//                }
//
//            }
//
//        }
//    }

    private void handleVoucherOrder(VoucherOrder voucherOder) {
        Long userId = voucherOder.getUserId();
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        boolean isLock = lock.tryLock();
        if(!isLock){
            //fail to get lock
            log.error("lock 异常");
            return;
        }
        try {
            proxy.createVoucherOrder(voucherOder);
        }finally {
            lock.unlock();
        }
    }
    private IVoucherOrderService proxy;
    @Override
    public Result seckillVoucher(Long voucherId) {
        //获取用户
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");
        //1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKIll_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString(),
                String.valueOf(orderId)
        );
        //2.判断return 是否 0
        int r = result.intValue();
        if (r != 0){
            //2.1 非0 -> 无资格
            return Result.fail(r == 1 ? "库存不足" : "用户一人一单");
        }
        //  获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        //返回order id
        return Result.ok(orderId);
    }

//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        //获取用户
//        Long userId = UserHolder.getUser().getId();
//        //1.执行lua脚本
//        Long result = stringRedisTemplate.execute(
//                SECKIll_SCRIPT,
//                Collections.emptyList(),
//                voucherId.toString(),
//                userId.toString()
//        );
//        //2.判断return 是否 0
//        int r = result.intValue();
//        if (r != 0){
//            //2.1 非0 -> 无资格
//            return Result.fail(r == 1 ? "库存不足" : "用户一人一单");
//        }
//            //2.2 0 -〉可以购买
//        long orderId = redisIdWorker.nextId("order");
//        //TODO 保存阻塞队列
//        VoucherOrder voucherOrder = new VoucherOrder();
//        long orderID = redisIdWorker.nextId("order");
//        voucherOrder.setId(orderID);
//
//        voucherOrder.setUserId(userId);
//        voucherOrder.setVoucherId(voucherId);
//        orderTasks.add(voucherOrder);
//        //  获取代理对象
//        proxy = (IVoucherOrderService) AopContext.currentProxy();
//        //返回order id
//        return Result.ok(orderId);
//    }
//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        //1.查询优惠券
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//        //2.判断：秒杀是否开始
//        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
//            return Result.fail("秒杀未开始");
//
//        }
//        //3。判断：秒杀是否结束
//        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
//            return Result.fail("秒杀已结束");
//
//        }
//        //4.判断：是否充足
//        if (voucher.getStock() < 1) {
//            return Result.fail("库存不足");
//        }
//        Long userID = UserHolder.getUser().getId();
//        //分布式锁
//      SimpleRedisLock lock = new SimpleRedisLock("order" + userID, stringRedisTemplate);
//        RLock lock = redissonClient.getLock("lock:order:" + userID);
//        boolean isLock = lock.tryLock();
//        if(!isLock){
//            //fail to get lock
//            return Result.fail("only one order per customer");
//        }
//        try {
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        }finally {
//            lock.unlock();
//        }
//    }
    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        //实现一人一单功能
        Long userID = voucherOrder.getUserId();
            int count = query().eq("user_id", userID).eq("voucher_id", voucherOrder).count().intValue();
            if (count > 0) {
                log.error("This user is already voucher");
                return;
            }
            //5.扣库存
            boolean success = seckillVoucherService.update().
                    setSql("stock =  stock - 1 ").
                    eq("voucher_id", voucherOrder).gt("stock", 0).update();
            if (!success) {
                log.error("库存不足");
            }

            save(voucherOrder);


    }
}
