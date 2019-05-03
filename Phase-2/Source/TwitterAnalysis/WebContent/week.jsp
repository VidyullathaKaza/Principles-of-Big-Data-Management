<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
<%@page import="java.util.List"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
<title>Insert title here</title>
<script src="https://ajax.googleapis.com/ajax/libs/jquery/1.12.0/jquery.min.js"></script>
    <script src="https://code.highcharts.com/highcharts.js"></script>
    <script src="https://code.highcharts.com/modules/data.js"></script>
    <script src="https://code.highcharts.com/modules/drilldown.js"></script>
<%
	List<String> keys = (List<String>) request.getAttribute("keys");
	List<Integer> values = (List<Integer>) request.getAttribute("values");
%>
<script language="JavaScript">
    var series = [];
    <%for (int i = 0; i < keys.size(); i++) {%>
        console.log(<%=keys.get(i)%>);
        series.push(['<%=keys.get(i)%>', <%=values.get(i)%>]);
    <%}%>
    console.log(series);
 </script>
</head>
<body>
<div id="container" style="min-width: 310px; height: 400px; margin: 0 auto"></div>

</body>
<script language="JavaScript">
$(function () {
    // Create the chart
    $('#container').highcharts({
        chart: {
            type: 'column'
        },
        title: {
            text: 'Users Tweeted On Which Day of Week'
        },
        subtitle: {
            text: ''
        },
        xAxis: {
            type: 'Week'
        },
        yAxis: {
            title: {
                text: 'No of tweets on Diseases'
            }

        },
        legend: {
            enabled: false
        },
        plotOptions: {
            series: {
                borderWidth: 0,
                dataLabels: {
                    enabled: true,
                    format: '{point.y:.1f}'
                }
            }
        },

        tooltip: {
            headerFormat: '<span style="font-size:11px">{series.name}</span><br>',
            pointFormat: '<span style="color:{point.color}">{point.name}</span>: <b>{point.y:.2f}%</b> of total<br/>'
        },

        series: [{
        	name: 'Week',
            colorByPoint: true,
            data: [
            	<%for (int i = 0; i < keys.size() - 1; i++) {%>
        {name:'<%=keys.get(i)%>', y:<%=values.get(i)%>},
    <%}%>
                {name:'<%=keys.get(keys.size() - 1)%>', y:<%=values.get(values.size() - 1)%>}
            ]
        }]
    });
});
</script>
</html>