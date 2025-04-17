package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.conditions.query.QueryChainWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import org.jetbrains.annotations.NotNull;
import org.springframework.aop.framework.AopContext;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    StringRedisTemplate stringRedisTemplate;
    @Override
    public Result seckillVoucher(Long voucherId) {
        //1.查询优惠券
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        //2.判断：秒杀是否开始
        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
            return Result.fail("秒杀未开始");

        }
        //3。判断：秒杀是否结束
        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
            return Result.fail("秒杀已结束");

        }
        //4.判断：是否充足
        if (voucher.getStock() < 1) {
            return Result.fail("库存不足");
        }
        Long userID = UserHolder.getUser().getId();
        SimpleRedisLock lock = new SimpleRedisLock("order" + userID, stringRedisTemplate);
        boolean isLock = lock.tryLock(1200);
        if(!isLock){
            //fail to get lock
            return Result.fail("only one order per customer");
        }
        try {
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        }finally {
            lock.unlock();
        }

    }
    @Transactional
    public Result createVoucherOrder(Long voucherId) {
        //实现一人一单功能
        Long userID = UserHolder.getUser().getId();
            int count = query().eq("user_id", userID).eq("voucher_id", voucherId).count().intValue();
            if (count > 0) {
                return Result.fail("This user is already voucher");
            }
            //5.扣库存
            boolean success = seckillVoucherService.update().
                    setSql("stock =  stock - 1 ").
                    eq("voucher_id", voucherId).gt("stock", 0).update();
            if (!success) {
                return Result.fail("库存不足");
            }
            //6.创订单
            VoucherOrder voucherOrder = new VoucherOrder();
            long orderID = redisIdWorker.nextId("order");
            voucherOrder.setId(orderID);

            voucherOrder.setUserId(userID);
            voucherOrder.setVoucherId(voucherId);
            save(voucherOrder);
            //7.返回订单id
            return Result.ok(orderID);

    }
}
