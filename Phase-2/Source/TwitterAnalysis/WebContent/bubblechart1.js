$(document).ready(function () {
    var bubbleChart = new d3.svg.BubbleChart({
        supportResponsive: true,
        //container: => use @default
        size: 600,
        //viewBoxSize: => use @default
        innerRadius: 600 / 3.5,
        //outerRadius: => use @default
        radiusMin: 50,
        //radiusMax: use @default
        //intersectDelta: use @default
        //intersectInc: use @default
        //circleColor: use @default
        data: {
            items: [
                {text: "amelia", count: "184334"},
                {text: "Nikko' s Mom", count: "109835"},
                {text: " Hannah Miller", count: "109833"},
                {text: " Dylan", count: "90509"},
                {text: "lumière astrale", count: "67013"},
                {text: "الفلاحي", count: "63756"},
                {text: "Mickey Taliel", count: "61893"},
                {text: "Dennis Fuire", count: "48516"},
                {text: "ⵀⵉⴱⴰ", count: "47536"},
            ],
            eval: function (item) {return item.count;},
            classed: function (item) {return item.text.split(" ").join("");}
        },
        plugins: [
            {
                name: "central-click",
                options: {
                    text: "",
                    style: {
                        "font-size": "12px",
                        "font-style": "italic",
                        "font-family": "Cambria",
                        //"font-weight": "700",
                        "text-anchor": "middle",
                        "fill": "black"
                    },
                    attr: {dy: "65px"},
                    centralClick: function() {
                        alert("Here is more details!!");
                    }
                }
            },
            {
                name: "lines",
                options: {
                    format: [
                        {// Line #0
                            textField: "count",
                            classed: {count: true},
                            style: {
                                "font-size": "28px",
                                "font-family": "Cambria",
                                "text-anchor": "middle",
                                fill: "black"
                            },
                            attr: {
                                dy: "0px",
                                x: function (d) {return d.cx;},
                                y: function (d) {return d.cy;}
                            }
                        },
                        {// Line #1
                            textField: "text",
                            classed: {text: true},
                            style: {
                                "font-size": "14px",
                                "font-family": "Cambria",
                                "text-anchor": "middle",
                                fill: "black"
                            },
                            attr: {
                                dy: "20px",
                                x: function (d) {return d.cx;},
                                y: function (d) {return d.cy;}
                            }
                        }
                    ],
                    centralFormat: [
                        {// Line #0
                            style: {"font-size": "50px"},
                            attr: {}
                        },
                        {// Line #1
                            style: {"font-size": "30px"},
                            attr: {dy: "40px"}
                        }
                    ]
                }
            }]
    });
});