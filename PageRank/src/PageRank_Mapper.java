public class PageRank_Mapperpublic class PageRankMapper extends Mapper<Text, Text, Text, Text> {

    private static final Log log = LogFactory.getLog(PageRankMapper.class);

    public static float factor = 0.85f;// this is the alpha in PageRank


    protected void setup(Context context) throws IOException,
            InterruptedException {
        // get the alpha, =0.85
        factor = context.getConfiguration().getFloat("mapred.pagerank.factor",
                0.85f);
    }


    protected void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {
        log.info(key.toString() + ";" + value.toString());
        String[] outLinks = value.toString().split(",");
        String[] link = key.toString().split(",");
        float rank = factor;
        if (link.length > 1) {
            rank = Float.parseFloat(link[1]);
        }
        int outLinkLen = outLinks.length;// outlinks from A

        for (String s : outLinks) {
            context.write(new Text(s), new Text(link[0] + ";" + rank + ";"
                    + outLinkLen));
        }

        context.write(new Text(link[0]), value);
    }
}
