package edu.umkc.pb;

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

@WebServlet("/cuisine")
public class DayServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public DayServlet() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		String inputFile = "/home/koushik/Downloads/tweets.json";
        SparkConf sparkConf = new SparkConf().setAppName("CuisineCounts").setMaster("local").set("spark.driver.allowMultipleContexts", "true");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        SQLContext sc = new SQLContext(ctx);

        DataFrame d = sc.jsonFile(inputFile);
        d.registerTempTable("tweets");
        DataFrame data,data1;
        data = sc.sql("SELECT substring(user.created_at,1,3) as day from tweets where text is not null");
        data.registerTempTable("texts");
        data1 = sc.sql("SELECT day from texts where (day LIKE '%Mon%'"
        		+ "or day LIKE '%Tue%'"
        		+ "or day LIKE '%Wed%'"
        		+ "or day LIKE '%Thu%'"
        		+ "or day LIKE '%Fri%'"
        		+ "or day LIKE '%Sat%'"
        		+ "or day LIKE '%Sun%')"
        		+ "and day is not null");
        long total = data1.count();
        System.out.println(data1.count() +" " + data.count());
        JavaRDD<String> words = data1.toJavaRDD().flatMap(
                new FlatMapFunction<Row, String>() {
                    @Override
                    public Iterable<String> call(Row row) throws Exception {
                        String s = "";
                         if (row.getString(0).contains("Mon"))
                                s = "MONDAY";
                            if (row.getString(0).contains("Tue"))
                                s = "TUESDAY";
                            if (row.getString(0).contains("Wed"))
                                s = "WEDNESDAY";
                            if (row.getString(0).contains("Thu"))
                                s = "THURSDAY";
                            if (row.getString(0).contains("Fri"))
                                s = "FRIDAY";
                            if (row.getString(0).contains("Sat"))
                                s = "SATURDAY";
                            if (row.getString(0).contains("Sun"))
                                s = "SUNDAY";
                        s.trim();
                        return Arrays.asList(s.split(" "));
                    }
                }
        );
        JavaPairRDD<String, Integer> counts = words.mapToPair(
                new PairFunction<String, String, Integer>(){
                    public Tuple2<String, Integer> call(String s){
                        return new Tuple2(s, 1);
                    }
                } );

        // Java 7 and earlier: count the words
        JavaPairRDD<String, Integer> reducedCounts = counts.reduceByKey(
                new Function2<Integer, Integer, Integer>(){
                    public Integer call(Integer x, Integer y){ return x + y; }
                } );
       
        List<String> keys = reducedCounts.keys().toArray();
        List<Integer> values = reducedCounts.values().toArray();
        ctx.stop();
        request.setAttribute("total", total);
        request.setAttribute("keys", keys);
        request.setAttribute("values", values);
        System.out.println(keys);
        System.out.println(values);
        RequestDispatcher rd = request.getRequestDispatcher("week.jsp");
        rd.forward(request, response);
    }

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}

}