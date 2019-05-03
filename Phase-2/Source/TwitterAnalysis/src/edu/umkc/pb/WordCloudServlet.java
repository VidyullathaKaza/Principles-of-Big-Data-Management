package edu.umkc.pb;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;

/**
 * Servlet implementation class WordCloudServlet
 */
@WebServlet("/wordcloud")
public class WordCloudServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;

	/**
	 * @see HttpServlet#HttpServlet()
	 */
	public WordCloudServlet() {
		super();
		// TODO Auto-generated constructor stub
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		// TODO Auto-generated method stub
		String inputFile = "/home/koushik/Downloads/tweets.json";
		String outputFile = getServletContext().getRealPath("/") + "/word.csv";
		File f = new File(outputFile);
		BufferedWriter bw;
		FileWriter fw = null;
		SparkConf sparkConf = new SparkConf().setAppName("CuisineCounts").setMaster("local")
				.set("spark.driver.allowMultipleContexts", "true");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		SQLContext sc = new SQLContext(ctx);

		DataFrame d = sc.jsonFile(inputFile);
		d.registerTempTable("tweets");
		DataFrame data, data1;
		data = sc.sql("select text from tweets where text is not null");
		JavaRDD<String> words = data.toJavaRDD().flatMap(new FlatMapFunction<Row, String>() {
			@Override
			public Iterable<String> call(Row row) throws Exception {
				String s = "";
				if ((row.getString(0).contains("trump") || row.getString(0).contains("Trump"))) {
					s = s + " " + "Trump";
				}
				if (row.getString(0).contains("president") || row.getString(0).contains("President"))
					s = s + " " + "President";
				if (row.getString(0).contains("america") || row.getString(0).contains("America"))
					s = s + " " + "America";
				if (row.getString(0).contains("great") || row.getString(0).contains("Great"))
					s = s + " " + "Great";
				if (row.getString(0).contains("donald") || row.getString(0).contains("Donald"))
					s = s + " " + "Donald";
				if (row.getString(0).contains("dollar") || row.getString(0).contains("Dollar"))
					s = s + " " + "Dollar";
				if (row.getString(0).contains("bitcoin") || row.getString(0).contains("Bitcoin"))
					s = s + " " + "Bitcoin";
				if (row.getString(0).contains("trust") || row.getString(0).contains("Trust"))
					s = s + " " + "Trust";
				if (row.getString(0).contains("job") || row.getString(0).contains("Job"))
					s = s + " " + "Job";
				if (row.getString(0).contains("H1-B") || row.getString(0).contains("H1-B"))
					s = s + " " + "H1-B";
				if (row.getString(0).contains("visa") || row.getString(0).contains("Visa"))
					s = s + " " + "Visa";
				if (row.getString(0).contains("politics") || row.getString(0).contains("Politics"))
					s = s + " " + "Politics";
				if (row.getString(0).contains("election") || row.getString(0).contains("Election"))
					s = s + " " + "Election";
				if (row.getString(0).contains("big") || row.getString(0).contains("Big"))
					s = s + " " + "Big";
				if (row.getString(0).contains("country") || row.getString(0).contains("Country"))
					s = s + " " + "Country";
				if (row.getString(0).contains("care") || row.getString(0).contains("Care"))
					s = s + " " + "Care";
				if (row.getString(0).contains("research") || row.getString(0).contains("Research"))
					s = s + " " + "Research";
				if (row.getString(0).contains("obama") || row.getString(0).contains("Obama"))
					s = s + " " + "Obama";
				if (row.getString(0).contains("barackobama"))
                    s = s+" " +"Barackobama";
                if (row.getString(0).contains("news"))
                	s = s + " " + "News";
                if (row.getString(0).contains("mexico wall"))
                	s = s + " " + "Mexicowall";
                if (row.getString(0).contains("immigration"))
                    s = s + " " +"Immigration";
                if (row.getString(0).contains("India"))
                	s = s + " " + "India";
                if (row.getString(0).contains("great"))
                	s = s + " " + "Great";
                if (row.getString(0).contains("Ivanka"))
                    s = s + " " +"Ivanka";
                if (row.getString(0).contains("clinton"))
                	s = s + " " + "Clinton";
                if (row.getString(0).contains("makeamericagreatagain"))
                    s = s + " " +"makeamericagreatagain";
                if (row.getString(0).contains("foxnews"))
                	s = s + " " + "Fox news";
                if (row.getString(0).contains("Hillary"))
                    s = s + " " +"Hillary";
                if (row.getString(0).contains("backtoday"))
                	s = s + " " + "Backtoday";
                if (row.getString(0).contains("tax"))
                    s = s + " " +"Tax";
                if (row.getString(0).contains("interview"))
                    s = s + " " +"Interview";
                if (row.getString(0).contains("vote") || row.getString(0).contains("Vote"))
                    s = s + " " +"vote";
                if (row.getString(0).contains("today") || row.getString(0).contains("Today"))
                    s = s + " " +"Today";
                if (row.getString(0).contains("foxandfriends"))
                    s = s + " " +"foxandfriends";
                if (row.getString(0).contains("whitehouse"))
                    s = s + " " +"whitehouse";
                if (row.getString(0).contains("Republic") || row.getString(0).contains("republic"))
					s = s + " " + "Republic";
				if (row.getString(0).contains("border") || row.getString(0).contains("Border"))
					s = s + " " + "Border";
				if (row.getString(0).contains("Twitter") || row.getString(0).contains("twitter"))
					s = s + " " + "twitter";
				if (row.getString(0).contains("fake") || row.getString(0).contains("Fake"))
					s = s + " " + "Fake";
				if (row.getString(0).contains("Nation") || row.getString(0).contains("nation"))
					s = s + " " + "Nation";
				if (row.getString(0).contains("Senate") || row.getString(0).contains("senate"))
					s = s + " " + "Senate";
				if (row.getString(0).contains("Justice") || row.getString(0).contains("justice"))
					s = s + " " + "Justice";
				if (row.getString(0).contains("War") || row.getString(0).contains("war"))
					s = s + " " + "War";
				if (row.getString(0).contains("security") || row.getString(0).contains("Security"))
					s = s + " " + "Security";
				if (row.getString(0).contains("Democrats") || row.getString(0).contains("democrats"))
					s = s + " " + "Democrats";
				if (row.getString(0).contains("Business") || row.getString(0).contains("business"))
					s = s + " " + "Business";
				if (row.getString(0).contains("Military") || row.getString(0).contains("military"))
					s = s + " " + "Military";
				s.trim();
				return Arrays.asList(s.split(" "));
			}
		});
		JavaPairRDD<String, Integer> counts = words.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2(s, 1);
			}
		});

		// Java 7 and earlier: count the words
		JavaPairRDD<String, Integer> reducedCounts = counts.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer x, Integer y) {
				return x + y;
			}
		});

		if (!f.exists()) {
			try {
				f.createNewFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		try {
			fw = new FileWriter(f);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		bw = new BufferedWriter(fw);
		bw.append("word,count");
		List<String> keys = reducedCounts.keys().toArray();
		List<Integer> values = reducedCounts.values().toArray();
		for (int i = 0; i < keys.size() - 1; i++) {
			bw.newLine();
			bw.append(keys.get(i) + "," + values.get(i));
			System.out.println(keys.get(i) + "," + values.get(i));
		}
		bw.flush();
		bw.close();
		ctx.stop();
		// request.setAttribute("total", total);
		RequestDispatcher rd = request.getRequestDispatcher("wordcloud.jsp");
		rd.forward(request, response);
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}

}