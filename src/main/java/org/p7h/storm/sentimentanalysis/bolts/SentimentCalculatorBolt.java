package org.p7h.storm.sentimentanalysis.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import org.p7h.storm.sentimentanalysis.utils.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.Status;
import twitter4j.URLEntity;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.logging.Level;
import wordcloud.CollisionMode;
import wordcloud.PolarBlendMode;
import wordcloud.PolarWordCloud;
import wordcloud.WordCloud;
import wordcloud.WordFrequency;
import wordcloud.bg.CircleBackground;
import wordcloud.bg.PixelBoundryBackground;
import wordcloud.font.scale.LinearFontScalar;
import wordcloud.nlp.FrequencyAnalizer;

/**
 * Breaks each tweet into words and calculates the sentiment of each tweet and
 * assocaites the sentiment value to the State and logs the same to the console
 * and also logs to the file.
 *
 * @author - Prashanth Babu
 */
public final class SentimentCalculatorBolt extends BaseRichBolt {

    private static final Logger LOGGER = LoggerFactory.getLogger(SentimentCalculatorBolt.class);
    private static final long serialVersionUID = 1942195527233725767L;
    private OutputCollector _outputCollector;

    private SortedMap<String, Integer> afinnSentimentMap = null;
    private SortedMap<String, Integer> stateSentimentMap = null;
    private SortedMap<String, String> stateTweetMap;
    //private List<String> myList;
    private List<String> myList1;
    private List<String> myList2;
    private SortedMap<String, String> negetiveSentimentMap = null;
    private SortedMap<String, String> positiveSentimentMap = null;
    int count = 0;

    public SentimentCalculatorBolt() {
    }

    @Override
    public final void prepare(final Map map, final TopologyContext topologyContext,
            final OutputCollector outputCollector) {
        afinnSentimentMap = Maps.newTreeMap();
        stateSentimentMap = Maps.newTreeMap();
        stateTweetMap = Maps.newTreeMap();
        negetiveSentimentMap = Maps.newTreeMap();
        positiveSentimentMap = Maps.newTreeMap();
        //myList = new ArrayList<>();
        myList1 = new ArrayList<>();
        myList2 = new ArrayList<>();
        this._outputCollector = outputCollector;

        //Bolt will read the AFINN Sentiment file [which is in the classpath] and stores the key, value pairs to a Map.
        try {
            final URL url = Resources.getResource(Constants.AFINN_SENTIMENT_FILE_NAME);
            final String text = Resources.toString(url, Charsets.UTF_8);
            final Iterable<String> lineSplit = Splitter.on("\n").trimResults().omitEmptyStrings().split(text);
            List<String> tabSplit;
            for (final String str : lineSplit) {
                tabSplit = Lists.newArrayList(Splitter.on("\t").trimResults().omitEmptyStrings().split(str));
                afinnSentimentMap.put(tabSplit.get(0), Integer.parseInt(tabSplit.get(1)));
            }
        } catch (final IOException ioException) {
            LOGGER.error(ioException.getMessage(), ioException);
            ioException.printStackTrace();
            //Should not occur. If it occurs, we cant continue. So, exiting at this point itself.
            System.exit(1);
        }
    }

    @Override
    public final void declareOutputFields(final OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("stateCode", "sentiment"));
    }

    private static InputStream getInputStream(String path) {
        InputStream in = null;
        try {
            in = new FileInputStream(new File(path));

        } catch (FileNotFoundException ex) {
            java.util.logging.Logger.getLogger(SentimentCalculatorBolt.class.getName()).log(Level.SEVERE, null, ex);
        }
        return in;
    }

    @Override
    public final void execute(final Tuple input) {
        final String state = (String) input.getValueByField("state");
        final Status status = (Status) input.getValueByField("tweet");
        final int sentimentOfCurrentTweet = getSentimentOfTweet(status);
        Integer previousSentiment = stateSentimentMap.get(state);
        previousSentiment = (null == previousSentiment) ? sentimentOfCurrentTweet : previousSentiment + sentimentOfCurrentTweet;
        stateSentimentMap.put(state, previousSentiment);
        String s = status.getText().replaceAll("\\p{Punct}|\\n", " ").trim().toLowerCase().replace("\\\\w\\\\d+", "").replace("fuck", "").replace("http", "");
//        System.out.println("s::::::" + s);
//        String j = stateTweetMap.get(state);
//        System.out.println("j :::::::::" + j + "s ::::: " + s);

        if (sentimentOfCurrentTweet > 0) {
            positiveSentimentMap.put(state, "" + positiveSentimentMap.get(state) + "\n" + s);
        } else if (sentimentOfCurrentTweet < 0) {
            negetiveSentimentMap.put(state, "" + negetiveSentimentMap.get(state) + "\n" + s);
        }
        //    stateTweetMap.put(state, j + "\n" + s);
        //      int ss = Constants.MAP_STATE_ID_CODE.size();
//      for (int i = 1; i < ss; i++) {

        String s1 = positiveSentimentMap.get(state);
        String s2 = negetiveSentimentMap.get(state);
        if (s1 != null && s2 != null) {
            myList1.add(s1.trim());
            myList2.add(s2.trim());
            try {
                FrequencyAnalizer frequencyAnalyzer = new FrequencyAnalizer();
                frequencyAnalyzer.setWordFrequencesToReturn(300);
                frequencyAnalyzer.setMinWordLength(4);
                frequencyAnalyzer.setStopWords(Constants.STOP_WORDS);
            //String s1 = stateTweetMap.get(state);
                //myList.add(s1.trim());
                //System.out.println(myList);

                //final List<WordFrequency> wordFrequencies = frequencyAnalyzer.load(myList);
                final List<WordFrequency> wordFrequencies1 = frequencyAnalyzer.load(myList1);
                final List<WordFrequency> wordFrequencies2 = frequencyAnalyzer.load(myList2);

                //final WordCloud wordCloud = new WordCloud(700, 500, CollisionMode.PIXEL_PERFECT);
                final PolarWordCloud wordCloud = new PolarWordCloud(600, 600, CollisionMode.PIXEL_PERFECT, PolarBlendMode.BLUR);
                wordCloud.setPadding(2);
//                wordCloud.setBackground(new PixelBoundryBackground(getInputStream("E:\\Git\\kumo\\src\\test\\resources\\backgrounds/whale_small.png")));
//                wordCloud.setColorPalette(new wordcloud.palette.ColorPalette(new Color(0x4055F1), new Color(0x408DF1), new Color(0x40AAF1), new Color(0x40C5F1), new Color(0x40D3F1), new Color(0xFFFFFF)));
                wordCloud.setBackground(new CircleBackground(300));
                wordCloud.setFontScalar(new LinearFontScalar(10, 40));
                wordCloud.build(wordFrequencies1, wordFrequencies2);
                File tmp_file = new File("E:/RealtimeTwitterSentiment/realtimetwittersentiment/web/assets/img/" + Constants.MAP_STATE_CODE_NAME.get(state) + ".png");
//                File tmp_file = new File("/opt/stormViz/web_alltime/assets/img/" + Constants.MAP_STATE_CODE_NAME.get(state) + ".png");
                if (tmp_file.exists()) {
                    tmp_file.delete();
                }
                try {
                    wordCloud.writeToFile("E:/RealtimeTwitterSentiment/realtimetwittersentiment/web/assets/img/" + Constants.MAP_STATE_CODE_NAME.get(state) + ".png");
//                    wordCloud.writeToFile("/opt/stormViz/web_alltime/assets/img/" + Constants.MAP_STATE_CODE_NAME.get(state) + ".png");
                } catch (Exception FileNotFoundException) {

                }
                myList1.clear();
                myList2.clear();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
//        }
        //int stateId = Constants.MAP_STATE_CODE_ID.get(state);
        _outputCollector.emit(new Values(state, previousSentiment));
        LOGGER.info("{}:{}", state, previousSentiment);
    }

    /**
     * Gets the sentiment of the current tweet.
     *
     * @param status -- Status Object.
     * @return sentiment of the current tweet.
     */
    private final int getSentimentOfTweet(final Status status) {
        //Remove all punctuation and new line chars in the tweet.
        final String tweet = status.getText().replaceAll("\\p{Punct}|\\n", " ").toLowerCase();
        //Splitting the tweet on empty space.
        final Iterable<String> words = Splitter.on(' ')
                .trimResults()
                .omitEmptyStrings()
                .split(tweet);
        int sentimentOfCurrentTweet = 0;
        //Loop thru all the wordsd and find the sentiment of this tweet.
        for (final String word : words) {
            if (afinnSentimentMap.containsKey(word)) {
                sentimentOfCurrentTweet += afinnSentimentMap.get(word);
            }
        }
        //LOGGER.debug("Tweet : Sentiment {} ==> {}", tweet, sentimentOfCurrentTweet);
        return sentimentOfCurrentTweet;
    }

    //Ideally we should be knocking off the URLs from the tweet since they don't need to parsed.
    private String filterOutURLFromTweet(final Status status) {
        final String tweet = status.getText();
        final URLEntity[] urlEntities = status.getURLEntities();
        int startOfURL;
        int endOfURL;
        String truncatedTweet = "";
        for (final URLEntity urlEntity : urlEntities) {
            startOfURL = urlEntity.getStart();
            endOfURL = urlEntity.getEnd();
            truncatedTweet += tweet.substring(0, startOfURL) + tweet.substring(endOfURL);
        }
        return truncatedTweet;
    }
}
