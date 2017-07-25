package me.wuzhaoyang;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.ZParams;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by john on 17/6/29.
 */
@SuppressWarnings("ALL")
public class Vote {
    private final static Jedis jedis = new Jedis("localhost");

    //一周的秒数
    private final static int ONE_WEEK_IN_SECONDS = 7 * 24 * 3600;

    //每票的分值
    private final static int VOTE_SCORE = 432;

    //每页大小
    private final static int PAGE_SIZE = 25;
    private static final String GROUP = "group:";
    private static final String ARTICLE = "article:";
    private static final String SCORE = "score:";
    private static final String TIME = "time:";
    private static final String VOTE = "vote:";

    //为文章投票,超过一周的文章不在支持投票
    public void articleVote(long userId, long articleId) {
        //校验文章距离发布时间是否超过一周
        Double articlePublishTime = jedis.zscore(TIME, ARTICLE + articleId);
        long now = System.currentTimeMillis();
        if (articlePublishTime + ONE_WEEK_IN_SECONDS < now) {
            return;
        }
        //判断该用户是否已经投票过
        if (jedis.sadd(VOTE + articleId, String.valueOf(userId)) == 1L) {
            //文章的投票数＋1，文章的评分数＋432
            jedis.hincrBy(ARTICLE + articleId, "votes", 1);
            jedis.zincrby(SCORE, VOTE_SCORE, ARTICLE + articleId);
        }

    }


    //发布文章
    public long postArticle(long userId, String title, String link) {
        long articleId = jedis.incr(ARTICLE);
        //将用户添加到投票列表中,并设置这个set的有效期为一个星期
        jedis.sadd(VOTE + articleId, String.valueOf(userId));
        jedis.expire(VOTE + articleId, ONE_WEEK_IN_SECONDS);

        long now = System.currentTimeMillis();
        jedis.hmset(ARTICLE + articleId, new HashMap<String, String>() {{
            put("title", title);
            put("link", link);
            put("poster", String.valueOf(userId));
            put("time", String.valueOf(now));
            put("votes", "1");

        }});
        //初始评分设定
        jedis.zadd(SCORE, now + VOTE_SCORE, ARTICLE + articleId);

        //文章发布时间
        jedis.zadd(TIME, now, ARTICLE + articleId);
        return articleId;
    }

    //分页获取文章 order取值为"time:"/"score:"
    public List<Map<String, String>> getArticles(int pageNo, String order) {
        int start = (pageNo - 1) * PAGE_SIZE;
        int end = start + PAGE_SIZE - 1;
        List<Map<String, String>> articles = new ArrayList<>();
        Set<String> articleIdPrefixSet = jedis.zrevrange(order, start, end);
        for (String articleIdPrefix : articleIdPrefixSet) {
            Map<String, String> articleData = jedis.hgetAll(articleIdPrefix);
            articleData.put("id", articleIdPrefix.split(":")[1]);
            articles.add(articleData);
        }
        return articles;
    }

    public List<Map<String, String>> txGetArticles(int pageNo, String order) {
        int start = (pageNo - 1) * PAGE_SIZE;
        int end = start + PAGE_SIZE - 1;
        List<Map<String, String>> articles = new ArrayList<>();
        Set<String> articleIdPrefixSet = jedis.zrevrange(order, start, end);

        List<String> idStr = articleIdPrefixSet.stream().map(s->s.split(":")[1]).collect(Collectors.toList());
        Transaction tx = jedis.multi();
        for (String articleIdPrefix : articleIdPrefixSet) {
            tx.hgetAll(articleIdPrefix);
        }
        List<Object> get = tx.exec();
        for (int i = 0, getSize = get.size(); i < getSize; i++) {
            Object o = get.get(i);
            Map<String, String> resultMap = (HashMap<String, String>) o;
            resultMap.put("id", idStr.get(i));
            articles.add(resultMap);
        }
        return articles;
    }

    public List<Map<String, String>> channelGetArticles(int pageNo, String order) {
        int start = (pageNo - 1) * PAGE_SIZE;
        int end = start + PAGE_SIZE - 1;
        List<Map<String, String>> articles = new ArrayList<>();
        Set<String> articleIdPrefixSet = jedis.zrevrange(order, start, end);

        List<String> idStr = articleIdPrefixSet.stream().map(s->s.split(":")[1]).collect(Collectors.toList());
        Pipeline pipelined = jedis.pipelined();
        for (String articleIdPrefix : articleIdPrefixSet) {
            pipelined.hgetAll(articleIdPrefix);
        }
        List<Object> get = pipelined.syncAndReturnAll();
        for (int i = 0, getSize = get.size(); i < getSize; i++) {
            Object o = get.get(i);
            Map<String, String> resultMap = (HashMap<String, String>) o;
            resultMap.put("id", idStr.get(i));
            articles.add(resultMap);
        }
        return articles;
    }

    //添加／删除群组
    public void addRemoveGroup(long articleId, String[] toAdd, String[] toRemove) {
        for (String addGroup : toAdd) {
            jedis.sadd(GROUP + addGroup, ARTICLE + articleId);
        }
        for (String removeGroup : toRemove) {
            jedis.srem(GROUP + removeGroup, ARTICLE + articleId);
        }
    }

    //分组获取文章 order取值为"time:"/"score:",group取值为组名
    public List<Map<String, String>> getGroupArticles(int pageNo, String order, String group) {
        //判断群组是否存在
        String key = order + group;
        if (!jedis.exists(key)) {
            ZParams zParams = new ZParams();
            zParams.aggregate(ZParams.Aggregate.MAX);
            jedis.zinterstore(key, zParams, order, GROUP + group);
            jedis.expire(key, 60);
        }
        return getArticles(pageNo, key);
    }

}
