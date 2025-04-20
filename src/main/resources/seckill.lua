---
--- Created by shuchangwen.
--- DateTime: 4/18/25 3:20 PM
---
-- 1. parameters list: voucher id, user id
local voucherId = ARGV[1]
local userId = ARGV[2]
local orderId = ARGV[3]
-- 2. stockKey and orderKey
local stockKey = 'seckill:stock:' .. voucherId
local orderKey = 'seckill:order:' .. voucherId

if (tonumber(redis.call('get', stockKey)) <= 0) then
    return 1
end

if (redis.call('sismember', orderKey, userId) == 1) then
    return 2
end

redis.call('incrby', stockKey, -1)
redis.call('sadd', orderKey, userId)

-- 发消到队列
redis.call('xadd', 'stream.orders', '*', 'userId', userId, 'voucherId', voucherId, 'id', orderId)

return 0