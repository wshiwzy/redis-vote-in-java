package me.wuzhaoyang;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.util.List;
import java.util.Map;

/**
 * Unit test for simple Vote.
 */
public class VoteTest
        extends TestCase {
    Vote vote = new Vote();

    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public VoteTest(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(VoteTest.class);
    }

    /**
     * Rigourous Test :-)
     */
    public void testVote() {
        long articleId = vote.postArticle(1L, "测试文案1", "www.baidu.com");
        vote.articleVote(2L, articleId);
        List<Map<String, String>> articles = vote.getArticles(1, "time:");
        System.out.print(articles);
    }
}
