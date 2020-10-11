import com.xycode.zkUtils.zkClient.ZKCli;
import org.apache.zookeeper.KeeperException;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashMap;

/*
 *
 * @author: xycode
 * @date: 2020/10/11
 */
public class ZKCliTest {

    @Test
    public void testZKCli() throws KeeperException, InterruptedException, IOException, ClassNotFoundException {
        //test
//        String path="/listener";
//        MyEventListener listener=new MyEventListener(path);
//        //一开始的Watcher不能为null,后面倒是可以了...
//        ZKCli zkCli=new ZKCli(Config.ZKC_ADDRESS, new Watcher() {
//            @Override
//            public void process(WatchedEvent event) {
//
//            }
//        });
//        try {
//            zkCli.waitConnected(3000);
//        } catch (TimeoutException e) {
//            e.printStackTrace();
//            return;
//        }
//        try {
//            zkCli.createPersistent(path,"", ZooDefs.Ids.OPEN_ACL_UNSAFE);
//            zkCli.writeStringData(path,"changed");
//            zkCli.registerListener(listener);//Zookeeper中的watcher不是一个集合,只是一个实例...,这里就替换了之前的空监听器
//            zkCli.writeStringData(path,"changed2");
//            zkCli.registerListener(new MyEventListener("/s"));//这里替换了对"/listener"的监听器,后续的delete "/listener"将不会监听到
//            zkCli.delete(path);
//
//            //试试新的"/s"的监听器,能监听到,说明成功替换了监听器
//            zkCli.createPersistent("/s","", ZooDefs.Ids.OPEN_ACL_UNSAFE);
//            zkCli.writeStringData("/s","changed3");
//            zkCli.unregisterListener();//注销之前的监听器,下面的delete事件将不会监听到
//            zkCli.delete("/s");
//
//            zkCli.close();
//        } catch (KeeperException e) {
//            e.printStackTrace();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }


//        ZKCli zkCli=new ZKCli(Config.ZKC_ADDRESS);
//        String path="/PersistentSeq";
//        try {
//            if(!zkCli.exists(path))
//                zkCli.createPersistent(path,"", ZooDefs.Ids.OPEN_ACL_UNSAFE);
//            zkCli.createEphemeralSeq(path+"/1","", ZooDefs.Ids.OPEN_ACL_UNSAFE);
//            String s=zkCli.createEphemeralSeq(path+"/1","", ZooDefs.Ids.OPEN_ACL_UNSAFE);
//            System.out.println(s);
//            zkCli.registerListener(new AbstractEventListener(s) {
//                @Override
//                protected void NodeCreatedHandler(WatchedEvent event) {
//
//                }
//
//                @Override
//                protected void NodeDeletedHander(WatchedEvent event) {
//                    System.out.println("[delete]: "+s);
//                }
//
//                @Override
//                protected void NodeDataChangedHandler(WatchedEvent event) {
//
//                }
//            });
////            List<String> l=zkCli.getChildren(path);
////            for(String data:l){
////                System.out.println(data);
////            }
//            zkCli.delete(s);
//            zkCli.close();
//        } catch (KeeperException e) {
//            e.printStackTrace();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

        //getChildren(),非递归方式获得子节点的名字集合
//        ZKCli zkCli=new ZKCli(Config.ZKC_ADDRESS);
//        for(String s:zkCli.getChildren("/a")){
//            System.out.println(s);
//        }

        //TODO: 测试readObject/writeObject的序列化效果
        ZKCli zkCli = new ZKCli("127.0.0.1:3181,127.0.0.1:3182,127.0.0.1:3183,127.0.0.1:3184");
        zkCli.createPersistent("/serializeTest");
        HashMap<String, String> mp = new HashMap<>();
        mp.put("1", "test1");
        mp.put("2", "test2");
        mp.put("三", "测试三");
        zkCli.writeObjectData("/serializeTest", mp);
        System.out.println(zkCli.readObjectData("/serializeTest"));
        zkCli.delete("/serializeTest");
        zkCli.close();
    }
}
