package com.sanjay.kafkatest2;

import java.util.HashSet;
import java.util.Set;
import com.twitter.clientlib.ApiClient;
import com.twitter.clientlib.ApiException;
import com.twitter.clientlib.TwitterCredentialsBearer;
import com.twitter.clientlib.api.TwitterApi;
import com.twitter.clientlib.model.ResourceUnauthorizedProblem;
import com.twitter.clientlib.model.Get2TweetsIdResponse;

/**
 * Initializing the TwitterApi using a non default ApiClient.
 */
public class NonDefaultClient {

    public static void main(String[] args) {
        NonDefaultClient example = new NonDefaultClient();
        // Create an ApiClient and use it instead of the default one in TwitterApi
        ApiClient apiClient = new ApiClient();
        apiClient.setTwitterCredentials(new TwitterCredentialsBearer("AAAAAAAAAAAAAAAAAAAAANWyigEAAAAAldJQ4mO11WEsUPp178JtdFbcsdI%3DQoLMrV6qpTuslW2gfVN8KjLqiBKRFAyI5q1G62WD2XvdsuaV4H"));
        TwitterApi apiInstance = new TwitterApi(apiClient);
        example.callApi(apiInstance);
    }

    public void callApi(TwitterApi apiInstance) {
        Set<String> tweetFields = new HashSet<>();
        tweetFields.add("author_id");
        tweetFields.add("id");
        tweetFields.add("created_at");

        try {
            // findTweetById
            Get2TweetsIdResponse result = apiInstance.tweets().findTweetById("759043035355312128")
                    .tweetFields(tweetFields)
                    .execute();
            if (result.getErrors() != null && result.getErrors().size() > 0) {
                System.out.println("Error:");
                result.getErrors().forEach(e -> {
                    System.out.println(e.toString());
                    if (e instanceof ResourceUnauthorizedProblem) {
                        System.out.println(e.getTitle() + " " + e.getDetail());
                    }
                });
            } else {
                System.out.println("findTweetById - Tweet Text: " + result.toString());
            }
        } catch (ApiException e) {
            System.err.println("Status code: " + e.getCode());
            System.err.println("Reason: " + e.getResponseBody());
            System.err.println("Response headers: " + e.getResponseHeaders());
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}