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
                {text: "Fresh MILF 11k", count: "4"},
                {text: "Global 🌍 ...", count: "4"},
                {text: "Exposed X TM", count: "3"},
                {text: "ØĐĐ's✴️ᗪOᗰIᑎᗩS ➌🅚", count: "3"},
                {text: "Crawford❌MAGA🇺🇸...", count: "3"},
                {text: " March for Progress", count: "2"},
                {text: "Karen Savage", count: "2"},
                {text: "Mark “FadingBeto”...", count: "2"},
                {text: "spring", count: "2"},
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