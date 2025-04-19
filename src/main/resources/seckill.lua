---
--- Created by shuchangwen.
--- DateTime: 4/18/25 3:20 PM
---
-- 1. parameters list: voucher id, user id
local voucherId = ARGV[1]
local userId = ARGV[2]
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

return 0