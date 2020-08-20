package org.example.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.Collections;
import java.util.UUID;

/**
 * Created by atyongsi@163.com on 2020/8/20
 * Description:使用redis实现分布式锁
 */
public class RedisDistributedLock {

    private static final Logger logger = LoggerFactory.getLogger(RedisDistributedLock.class);

    private static final long WAIT_TIME = 3 * 1000;//3秒
    private static final int EXPIRE_TIME = 5;//加锁后锁的过期时间,超过5秒自动释放
    private static final String LOCK_SUCCESS = "OK";//加锁成功后,redis客户端返回OK
    private static final Long RELEASE_SUCCESS = 1L;//删除锁成功后的返回值

    private JedisPool jedisPool;
    private String lockKey;

    public RedisDistributedLock(JedisPool jedisPool, String lockKey) {
        this.jedisPool = jedisPool;
        this.lockKey = lockKey;
    }

    //秒杀服务加锁
    public String secKillLock() {
        Jedis jedis = jedisPool.getResource();//获取连接
        //请求加锁的时候,如果超过等待时间,加锁失败,等待下一次请求
        long waitEnd = System.currentTimeMillis() + WAIT_TIME;
        String value = UUID.randomUUID().toString();//随机产生的value值
        if (System.currentTimeMillis() < waitEnd) {
            String result = jedis.setex(lockKey, EXPIRE_TIME, value);
            if (LOCK_SUCCESS.equals(result)) {
                return value;
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        return null;
    }

    //释放锁
    public Boolean releaseLock(String value) {
        Jedis jedis = jedisPool.getResource();
        if (value == null) {//如果没有value值,说明没有加锁,就不需要释放锁
            return false;
        }
        try {
            //判断Key存在并且删除key是一个原子操作,这里使用lua脚本,让判断和删除2步骤形成原子操作
            String luaScript = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
            Object result = jedis.eval(luaScript, Collections.singletonList(lockKey), Collections.singletonList(value));

            if (RELEASE_SUCCESS.equals(result)) {
                logger.info("release lock success, value:{}", value);
                return true;
            }
        } catch (Exception e) {
            logger.error("release lock error", e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        logger.info("release lock failed, value:{}, result:{}", value);
        return false;
    }

    public static void main(String[] args) {
        System.out.println(System.currentTimeMillis());
        System.out.println(System.currentTimeMillis() + 3000);
    }

}
