package cn.wuyl.frame.db.mongodb.demo;

import cn.wuyl.frame.db.mongodb.base.MongoConst;
import cn.wuyl.frame.db.mongodb.base.MongoDao;
import cn.wuyl.frame.db.mongodb.base.MongoDaoImpl;
import cn.wuyl.frame.db.mongodb.base.MongoHelper;
import cn.wuyl.frame.utils.string.PinyinUtil;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

@Slf4j
public class MongDBDemo {

    public static void main( String args[] ){
        String[] host = {"dxnb.top"};
        int[] port = {27017};
        String databaseName = "weather";
        String userName = "weather";
        String password = "123456";
        String table = "weather";

        MongoHelper mongoHelper = new MongoHelper(host,port,databaseName,userName,password);
        MongoClient mongoClient = mongoHelper.getMongoClient();
        MongoDatabase mongoDatabase = mongoHelper.getMongoDataBase(mongoClient);
        MongoDao dao = new MongoDaoImpl();
        try {
            //增加一条记录
//            Document document = new Document("city", "武汉").append("pinyin", PinyinUtil.hanziToPinyin("武汉")).
//                    append("date", "2018/11/27").
//                    append("minimumTemperature", "7").
//                    append("maximumTemperature", "13").append("weather","晴").append("windDirection","西北风").append("windPower","小于3级");
//            dao.insert(mongoDatabase,table,document);
            //删除一条记录
//            dao.delete(mongoDatabase, table,Filters.eq("ctiy","武汉"));
//            dao.delete(mongoDatabase, table,Filters.eq("pinyin",PinyinUtil.hanziToPinyin("武汉")));
//            dao.delete(mongoDatabase, table,Filters.eq("date","2018/11/27"));

            //查询
            System.out.println("武汉");
            //日期date 大于 "2017-01-01" 并且 小于 "2017-02-30"
            BasicDBList basicDBList = new BasicDBList();
            basicDBList.add(new BasicDBObject("date",new BasicDBObject(MongoConst.GTE.getCompareIdentify(),"2017-01-01")));
            basicDBList.add(new BasicDBObject("date",new BasicDBObject(MongoConst.LTE.getCompareIdentify(),"2017-02-30")));
            
//            BasicDBObject doc =new BasicDBObject().append("date","2017-01-01").append("pinyin","wu han");
//            BasicDBObject doc =new BasicDBObject().append("date",new BasicDBObject(MongoConst.GTE.getCompareIdentify(),"2017-01-01")).append("pinyin","wu han");
            //日期date 大于 "2017-01-01" 并且 小于 "2017-02-30" 并且 城市pinyin 等于 "wu han"
            BasicDBObject doc =new BasicDBObject().append(MongoConst.AND.getCompareIdentify(),basicDBList)//日期date 大于 "2017-01-01" 并且 小于 "2017-02-30"
                    .append("pinyin","wu han");//城市pinyin 等于 "wu han"
            List<Map<String, Object>> maps = dao.queryByDoc(mongoDatabase, table,doc);

/*            //日期date 大于 "2017-01-01" 并且 小于 "2017-02-30" 并且 城市pinyin 等于 "wu han"
            BasicDBObject query =new BasicDBObject();
//            query.put("pinyin", PinyinUtil.hanziToPinyin("武汉"));
            query.put("pinyin", "wu han");//城市pinyin 等于 "wu han"
            query.put(MongoConst.AND.getCompareIdentify(),basicDBList);//日期date 大于 "2017-01-01" 并且 小于 "2017-02-30"
            List<Map<String, Object>> maps = dao.queryByDoc(mongoDatabase, table,query);*/
            System.out.println(table+" size:"+maps.size());
            //遍历
            for (Map map:maps){
                System.out.println(map.toString());
            }

        } catch (Exception e) {
//            log.error(e.getMessage());
            e.printStackTrace();
        }finally {
            mongoHelper.closeMongoClient(mongoDatabase,mongoClient);
        }
    }

}
