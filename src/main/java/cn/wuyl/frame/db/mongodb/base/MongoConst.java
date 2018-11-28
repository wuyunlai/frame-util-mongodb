package cn.wuyl.frame.db.mongodb.base;

public enum MongoConst {
    GT("$gt","大于"),
    LT("$lt","小于"),
    GTE("$gte","大于等于"),
    LTE("$lte","小于等于"),
    AND("$and","并且"),
    OR("$or","或者"),
    NOT("$not","非"),
    IN("$in","在里面");
    private String compareIdentify;
    private String compareDescription;

    MongoConst(String compareIdentify, String compareDescription) {
        this.compareIdentify = compareIdentify;
        this.compareDescription = compareDescription;
    }
    public String getCompareIdentify() {
        return compareIdentify;
    }
    public String getCompareDescription() {
        return compareDescription;
    }
}
