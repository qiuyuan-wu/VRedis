import redis.clients.jedis.VshardedJedis;
import redis.clients.jedis.VshardedJedisPipeline;
import redis.clients.jedis.VshardedJedisPool;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: qiuyuan.wu
 * Date: 15-1-22
 * Time: 下午12:17
 * To change this template use File | Settings | File Templates.
 */
public class TestThread implements Runnable {
    private VshardedJedisPool pool;
    private String name;
    private String key;

    public TestThread(VshardedJedisPool pool,String name,String key){
        this.pool = pool;
        this.name = name;
        this.key = key;

    }
    @Override
    public void run() {
        int inx = 0;
        while (true){
            inx++;
            System.out.println("================pipeline==================="+name+":"+inx);
            VshardedJedis jedis = pool.getResource();
            VshardedJedisPipeline pipeline =  jedis.pipelined();
            VshardedJedisPipeline pipelineMap =  jedis.pipelined();
            VshardedJedisPipeline pipelineList =  jedis.pipelined();

            for(int i=0;i<1000003;i++){
                if(i % 10000==0){
                    pipeline.sadd(key,"value_"+i);
                    pipelineMap.hset(key+"_map","name","value_"+i);
                    pipelineList.lpush(key+"_list","value_"+i);
                    pipeline.sync();
                    pipelineList.sync();
                    pipelineMap.sync();
                    pool.returnResource(jedis);
                    jedis = pool.getResource();
                    pipeline =  jedis.pipelined();
                    pipelineList =  jedis.pipelined();
                    pipelineMap =  jedis.pipelined();
                }else{
                    pipeline.sadd(key,"value_"+i);
                }

            }
            pipeline.sync();
            pool.returnResource(jedis);

            jedis = pool.getResource();
            Long len = jedis.scard(key);
            pool.returnResource(jedis);
            System.out.println(name+" set size:"+len);

            while(len>0l){
                String[] keys = new String[Integer.parseInt(len+"")];
                for(int i=0;i<10000;i++){
                    if(i<len)
                        keys[i]=key;
                }
                jedis = pool.getResource();
                List<Object> list = jedis.spopEx(pool,keys);
                pool.returnResource(jedis);
                System.out.println(name+" result size:"+list.size());

                jedis = pool.getResource();
                len = jedis.scard(key);
                pool.returnResource(jedis);
                System.out.println(name+" set size:"+len);
            }

            try {
                Thread.sleep(1000l);
            } catch (InterruptedException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }
    }
}
