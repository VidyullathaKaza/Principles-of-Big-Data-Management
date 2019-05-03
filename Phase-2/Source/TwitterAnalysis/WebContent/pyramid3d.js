$(function () {

    $('#container').highcharts({
        chart: {
            type: 'pyramid',
            marginRight: 100
        },
        title: {
            text: 'Top 10 Users Tweeted on Trump',
            x: -50
        },
        plotOptions: {
            series: {
                dataLabels: {
                    enabled: true,
                    format: '<b>{point.name}</b> ({point.y:,.0f})',
                    color: (Highcharts.theme && Highcharts.theme.contrastTextColor) || 'black',
                    softConnector: true
                }
            }
        },
        legend: {
            enabled: false
        },
        series: [{
            name: 'Tweet Count',
            data: [
                ['RafaelAntonioBMW',3], 
                ['FirstPressNG',1],
                ['Anandh kumar',6],
                ['REP.patrick Ramirez',5],
                ['Exposed X TM',3],
                ['Hollywood InkSlin...',4],
                ['Susan A Hanlon',1],
                ['Terry Dickenson',6], 
                ['Dr. Waleed Al-Salem',3],
                ['Trump Causes',4], 
            ]
        }]
    });
});