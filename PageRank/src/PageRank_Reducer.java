public class PageRankReducer extends Reducer<Text, Text, Text, Text> {
    private static final Log log = LogFactory.getLog(PageRankReducer.class);

    public static float factor = 0.85f;

    protected void setup(Context context) throws IOException,
            InterruptedException {
        factor = context.getConfiguration().getFloat("mapred.pagerank.factor",
                0.85f);
    }

    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        log.info(key.toString());
        float rank = 1 - factor;// PageRank score
        String[] str;
        Text outLinks = new Text();
        for (Text t : values) {
            str = t.toString().split(";");
            if (str.length == 3) {
                rank += Float.parseFloat(str[1]) / Integer.parseInt(str[2])
                        * factor;
            } else {
                outLinks.set(t.toString());
            }
        }
        context.write(new Text(key.toString() + "," + rank), outLinks);
    }
}