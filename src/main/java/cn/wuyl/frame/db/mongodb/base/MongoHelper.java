package cn.wuyl.frame.db.mongodb.base;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class MongoHelper {
    static String DBNAME = "weather";
    static String[] SERVERADDRESS = {"www.dxnb.top"};
    static int[] PORT = {27017};
    static String USERNAME = "weather";
    static String PASSWORD = "123456";

    public MongoHelper() {
    }

    public MongoHelper(String[] host, int[] port, String dbName) {
        this.DBNAME = dbName;
        this.SERVERADDRESS = host;
        this.PORT = port;
    }

    public MongoHelper(String[] host, int[] port, String dbName, String userName, String password) {
        this.DBNAME = dbName;
        this.SERVERADDRESS = host;
        this.PORT = port;
        this.USERNAME = userName;
        this.PASSWORD = password;
    }


    public MongoClient getMongoClient() {
        MongoClient mongoClient = null;
        try {
            //连接到MongoDB服务 如果是远程连接可以替换“localhost”为服务器所在IP地址
            //ServerAddress()两个参数分别为 服务器地址 和 端口
            List<ServerAddress> addrs = new ArrayList<ServerAddress>();
            for(int i = 0; i < SERVERADDRESS.length; i++){
                String host = SERVERADDRESS[i];
                int port = PORT.length>i?PORT[i]:PORT[PORT.length-1];
                ServerAddress serverAddress = new ServerAddress(host,port);
                addrs.add(serverAddress);
            }
            // 连接到 mongodb 服务
            mongoClient = new MongoClient(addrs);
            log.debug("Connect to mongodb successfully");
        } catch (Exception e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
        }
        return mongoClient;
    }

    public MongoDatabase getMongoDataBase(MongoClient mongoClient) {
        MongoDatabase mongoDataBase = null;
        try {
            if (mongoClient != null) {
                // 连接到数据库
                mongoDataBase = mongoClient.getDatabase(DBNAME);
                log.debug("Connect to DataBase successfully");
            } else {
                throw new RuntimeException("MongoClient不能够为空");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return mongoDataBase;
    }

    public MongoDatabase getMongoDataBase() {
        MongoDatabase mongoDataBase = null;
        try {
            // 连接到数据库
            mongoDataBase = getMongoDataBase(getMongoClient());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return mongoDataBase;
    }

    public void closeMongoClient(MongoDatabase mongoDataBase,
                                 MongoClient mongoClient) {
        if (mongoDataBase != null) {
            mongoDataBase = null;
        }
        if (mongoClient != null) {
            mongoClient.close();
        }
        log.debug("CloseMongoClient successfully");
    }
}
