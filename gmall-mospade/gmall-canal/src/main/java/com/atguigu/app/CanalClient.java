package com.atguigu.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.constants.GmallConstants;
import com.atguigu.utils.MykafkaSender;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;


import java.net.InetSocketAddress;
import java.util.List;
import java.util.Random;

public class CanalClient {
    public static void main(String[] args) throws InterruptedException, InvalidProtocolBufferException {

        //1.第一个参数,插座地址需要是SocketAddess类的对象,所以new一个它的子类InetSocketAddress
        InetSocketAddress socketAddress = new InetSocketAddress("mospade202", 11111);

        //2.获取canal连接对象
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(socketAddress, "example", "", "");

        while(true){
            //3.开启连接
            canalConnector.connect();

            //4.订阅数据库
            canalConnector.subscribe("gmall.*");

            //5.获取多个sql封装的数据
            Message message = canalConnector.get(100);

            //6.获取一个sql封装的数据
            List<CanalEntry.Entry> entries = message.getEntries();

            //7.有可能没有数据,做一个判断
            if(entries.size()<=0){
                System.out.println("没有数据,请等待!");
                Thread.sleep(5000);
            }else{
                //8.获取集合中的每一个entry
                for (CanalEntry.Entry entry : entries) {
                    //9.获取表明
                    String tableName = entry.getHeader().getTableName();
                    
                    //10.获取entry类型
                    CanalEntry.EntryType entryType = entry.getEntryType();

                    //11.根据entry类型获取序列化数据,将CanalEntry.EntryType中的ROWDATA类型和获取到的类型做比较
                    if(CanalEntry.EntryType.ROWDATA.equals(entryType)){
                        //12.获取序列化数据
                        ByteString storeValue = entry.getStoreValue();

                        //13.获取反序列化数据
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);

                        //14.获取事件类型
                        CanalEntry.EventType eventType = rowChange.getEventType();

                        //15.获取具体的数据
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

                        //16.根据表名以及事件类型获取不同的数据
                        handler(tableName,eventType,rowDatasList);


                    }
                }
            }
        }
    }

    private static void handler(String tableName,CanalEntry.EventType eventType,List<CanalEntry.RowData> rowDatasList) {

        //获取订单表新增数据
        if("order_info".equals(tableName)&&CanalEntry.EventType.INSERT.equals(eventType)){
            saveToKafka(rowDatasList, GmallConstants.KAFKA_TOPIC_ORDER);
            //获取订单明细表数据
        }else if ("order_detail".equals(tableName)&&CanalEntry.EventType.INSERT.equals(eventType)){
            saveToKafka(rowDatasList, GmallConstants.KAFKA_TOPIC_ORDER_DETAIL);
            //获取用户表数据
        }else if("user_info".equals(tableName)&&CanalEntry.EventType.INSERT.equals(eventType)||CanalEntry.EventType.UPDATE.equals(eventType)) {
            saveToKafka(rowDatasList, GmallConstants.KAFKA_TOPIC_USER);
        }
    }

    private static void saveToKafka(List<CanalEntry.RowData> rowDatasList, String kafkaTopicOrder) {
        for (CanalEntry.RowData rowData : rowDatasList) {
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();

            //创建JSONObject用来存放每一列的列名和列值
            JSONObject jsonObject = new JSONObject();

            for (CanalEntry.Column column : afterColumnsList) {
                jsonObject.put(column.getName(), column.getValue());
            }
            System.out.println(jsonObject.toString());

            //模拟网络震荡,数据延迟
/*            try {
                Thread.sleep(new Random().nextInt(5));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }*/

            //将封装后的JSON字符串写入kafka
            MykafkaSender.send(kafkaTopicOrder, jsonObject.toString());
        }
    }
}
