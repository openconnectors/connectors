package org.openconnectors.twitter;

import com.twitter.hbc.core.Constants;

public class TwitterFireHoseConfig {
    static final class Defaults {
        static final String CLIENT_NAME     = "openconnector-twitter-source";
        static final String CLIENT_HOSTS    = Constants.STREAM_HOST;
        static final int CLIENT_BUFFER_SIZE = 50000;
    }

    public static final String CONSUMER_KEY = "twitter-source.consumerKey";

    public static final String CONSUMER_SECRET = "twitter-source.consumerSecret";

    public static final String TOKEN = "twitter-source.token";

    public static final String TOKEN_SECRET = "twitter-source.tokenSecret";

    // ------ Optional property keys

    public static final String CLIENT_NAME = "twitter-source.name";

    public static final String CLIENT_HOSTS = "twitter-source.hosts";

    public static final String CLIENT_BUFFER_SIZE = "twitter-source.bufferSize";

    public static final String TWITTER_CONNECTOR_VERION = "0.0.1";
}
