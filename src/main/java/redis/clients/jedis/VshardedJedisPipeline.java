package redis.clients.jedis;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: qiuyuan.wu
 * Date: 14-12-26
 * Time: 上午11:53
 * To change this template use File | Settings | File Templates.
 */
public class VshardedJedisPipeline extends ShardedJedisPipeline {

    private VshardedJedis jedis;
    private List<FutureResult> results = new ArrayList<FutureResult>();
    private Queue<Client> clients = new LinkedList<Client>();

    private List<String> keys = new ArrayList<String>();

    private static Random rd = new Random();


    private static class FutureResult {
        private Client client;

        public FutureResult(Client client) {
            this.client = client;
        }

        public Object get() {
            return client.getOne();
        }
    }

    public void setShardedJedis(VshardedJedis jedis) {
        this.jedis = jedis;
    }


    public List<Object> getResults() {
        List<Object> r = new ArrayList<Object>();
        for (FutureResult fr : results) {
            r.add(fr.get());
        }
        return r;
    }

    /**
     * Syncronize pipeline by reading all responses. This operation closes the
     * pipeline. In order to get return values from pipelined commands, capture
     * the different Response&lt;?&gt; of the commands you execute.
     */
    public void sync() {
        for (Client client : clients) {
            generateResponse(client.getOne());
        }
    }

    /**
     * Syncronize pipeline by reading all responses. This operation closes the
     * pipeline. Whenever possible try to avoid using this version and use
     * ShardedJedisPipeline.sync() as it won't go through all the responses and
     * generate the right response type (usually it is a waste of time).
     *
     * @return A list of all the responses in the order you executed them.
     */
    public List<Object> syncAndReturnAll() {
        List<Object> formatted = new ArrayList<Object>();
        for (Client client : clients) {
            formatted.add(generateResponse(client.getOne()).get());
        }
        return formatted;
    }

    /**
     * This method will be removed in Jedis 3.0. Use the methods that return
     * Response's and call sync().
     */
    @Deprecated
    public void execute() {
    }

    @Override
    protected Client getClient(String key) {
        Client client = jedis.getShard(key).getClient();
        clients.add(client);
        results.add(new FutureResult(client));
        return client;
    }
    protected  Client getClient(int i){

        int index = 0;
        for(Jedis j:jedis.getAllShards()){
            if(i == index){
                Client client = j.getClient();
                clients.add(client);
                results.add(new FutureResult(client));
                return client;
            }
            //
            index++;
        }
        return null;
    }

    @Override
    protected Client getClient(byte[] key) {
        Client client = jedis.getShard(key).getClient();
        clients.add(client);
        results.add(new FutureResult(client));
        return client;
    }

    public Response<Long> sadd(String key, String member) {
        Client c = getClient(member);
        c.sadd(key, member);
        results.add(new FutureResult(c));
        return getResponse(BuilderFactory.LONG);
    }

    @Deprecated
    public Response<String> spop(String key){
        return null;
    }
    @Deprecated
    public Response<byte[]> spop(byte[] key){
        return null;
    }

    protected Response<String> spopEx(String key){
        keys.add(key);
        return null;
    }
    protected void clean(){
        keys.clear();
    }
    /**
     * 1.获取client、执行命令、返回结果；
     * 2.如果结果数量等于keys.size则返回；
     * 3.计算nil位置，并从第一步开始;
     * 4.结束
     * @return
     */
    protected List<Object> syncAndReturnAlls(int i) {
        List<Object> formatted = new ArrayList<Object>();

        List<Object> tmpList = syncAll(i,keys);
        if(tmpList != null && tmpList.size() >0){
            for(Object obj :tmpList){
                if(obj == null){
                    break;
                }else{
                    formatted.add(obj);
                }
            }
        }
        return formatted;
    }

    private List<Object> syncAll(int clientIndex, List<String> allKeys){

        List<Object> formatted = null;
        //执行命令
        int inx = 0;
        for(String key:allKeys){
            Client c = getClient(clientIndex);
            if(c != null){
                c.spop(key);
                results.add(new FutureResult(c));
                getResponse(BuilderFactory.STRING);
            }else{
                new Response<String>(BuilderFactory.STRING);
            }
            inx++;
        }
        //获取结果
        formatted = new ArrayList<Object>();
        for (Client client : clients) {
            Response<?> tmp = generateResponse(client.getOne());
            if(tmp != null){
                formatted.add(tmp.get());
            }
        }
        return formatted;
    }

    public Response<Long> srem(String key, String member) {
        Client c = getClient(member);
        c.srem(key, member);
        results.add(new FutureResult(c));
        return getResponse(BuilderFactory.LONG);
    }
}
