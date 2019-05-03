google.charts.load('current', {'packages':['geochart']});
google.charts.setOnLoadCallback(drawRegionsMap);

function drawRegionsMap() {

    var data = google.visualization.arrayToDataTable([
        ['Country', 'Count'],
        ['US', 40],
        ['GB', 39],
        ['FR', 30],
        ['CA', 28],
        ['ES', 24],
        ['AU', 23],
        ['ID', 17],
        ['MX', 17],
        ['CM', 15],
        ['AR', 15],
        ['ZA', 14],
        ['NG', 14],
        ['CO', 14],
        ['IN', 14],
        ['MY', 13],
        ['BR', 13],
        ['PH', 12],
        ['AT', 12],
        ['VE', 12],
        ['NL', 11],
    ]);

    var options = {};

    var chart = new google.visualization.GeoChart(document.getElementById('regions_div'));

    chart.draw(data, options);
}
