/*
 * QCRI, NADEEF LICENSE
 * NADEEF is an extensible, generalized and easy-to-deploy data cleaning platform built at QCRI.
 * NADEEF means "Clean" in Arabic
 *
 * Copyright (c) 2011-2013, Qatar Foundation for Education, Science and Community Development (on
 * behalf of Qatar Computing Research Institute) having its principle place of business in Doha,
 * Qatar with the registered address P.O box 5825 Doha, Qatar (hereinafter referred to as "QCRI")
 *
 * NADEEF has patent pending nevertheless the following is granted.
 * NADEEF is released under the terms of the MIT License, (http://opensource.org/licenses/MIT).
 */

define(
    ["text!mvc/template/table.template.html"],
    function (TableTemplate) {
    var svg;
    var width;
    var height;

    var firstPosition;
    var firstNode;
    var secondPosition;
    var secondNode;

    var table1;
    var table2;

    var conns = [];
    var pathPoints = [];
    var traceData = [];
    var traceSvg;

    var rainbow = [ "#00008B", "#8B0000", "#FF8C00", "BlueViolet", "Black", "#0000CD" ];
    var currentSelection;

    var onLineClickHandlers = [];
    var onLineAddedHandlers = [];

    function translate(x, y) {
        return 'translate(' + (x) + ',' + (y) + ')';
    }

    function onLineClick(f) {
        if (!_.isFunction(f)) {
            console.log("event handler needs to be a function");
            return;
        }

        onLineClickHandlers.push(f);
    }

    function onLineAdded(f) {
        if (!_.isFunction(f)) {
            console.log("event handler needs to be a function");
            return;
        }

        onLineAddedHandlers.push(f);
    }

    function Table(json, leftness, x, y) {
        this.name = json.name;
        this.columns = json.columns;
        this.x = x;
        this.y = y;
        this.leftness = leftness;
        this.boxWidth = 180;
        this.boxHeight = 30;
    }

    var pathGenerator = d3.svg.line()
        .x(function(d) { return d.x; })
        .y(function(d) { return d.y; })
        .interpolate("basis");

    function calcPoint(d) {
        var type = d[0];
        var x;
        var y;
        var index = parseInt(d.substr(1)) + 1;
        if (type == 'l') {
            x = table1.x + table1.boxWidth + 10;
            y = table1.y + table1.boxHeight * index + table1.boxHeight * 0.5;
        } else {
            x = table2.x - 10;
            y = table2.y + table2.boxHeight * index + table2.boxHeight * 0.5;
        }
        return { "x" : x, "y" : y };
    }

    function calcPath() {
        pathPoints = [];
        for (var i = 0; i < conns.length; i ++) {
            var p0 = calcPoint(conns[i][0]);
            var p3 = calcPoint(conns[i][1]);
            var p1 = { "x" : p0.x + 15, "y" : p3.y > p0.y ? p0.y + 23 : p0.y - 23};
            var p2 = { "x" : p3.x - 15, "y" : p3.y > p0.y ? p3.y - 23 : p3.y + 23};
            var points = [p0, p1, p2, p3];
            var key = conns[i][0] + conns[i][1];
            pathPoints[i] = { "key" : key, "points" : points };
        }
    }

    function drawTrace(e) {
        if (firstNode == null)
            return;

        traceData[0] = [calcPoint(firstNode.id), { "x" : e.x, "y" : e.y }];
        var tracePath =
            traceSvg
                .selectAll("path")
                .data(
                    traceData,
                    function() { return e.x * e.y; }
                );

        tracePath.enter().append("path");
        tracePath.exit().remove();
        tracePath
            .attr("stroke-width", 2)
            .attr("fill", "none")
            .attr("stroke",
                function() { return rainbow[conns.length % rainbow.length];  }
            ).attr("d", function(d) {
                return pathGenerator(d);
            });
    }

    function updateLine() {
        calcPath();
        var paths =
            svg.selectAll("path").data(pathPoints, function(d) { return d.key; });

        paths.enter().append("path");
        paths.exit().remove();
        paths.attr("d", function(d) { return pathGenerator(d.points); })
            .attr("stroke", function(d, i) { return rainbow[i % rainbow.length]; })
            .attr("id", function(d, i) { return i; })
            .attr("stroke-width", 2)
            .attr("fill", "none")
            .on('click', function(d) {
                console.log('clicked a line');
                _.each(lineClickHandlers, function (f) {
                    f(d);
                });
            })
            .on("mouseover",
                function() {
                    d3.select(this).style("stroke-width", 5)
                })
            .on("mouseout",
                function() {
                    d3.select(this).style("stroke-width", 2);
                }
        );
    }

    function start(r1, r2){
        table1 = new Table(r1, false, 0, 0);
        table2 = new Table(r2, true, 400, 0);

        render();
        bindEvent();
    }

    function drawTable(table) {
        var marginX = width * 0.1;
        var marginY = height * 0.1;

        var boxOffsetX = 5;
        var boxOffsetY = 5;

        var fontSize = 20;
        var fontOffsetX = 10;
        var fontOffsetY = fontSize;

        table.x += marginX;
        table.y += marginY;
        var leftness =  table.leftness;

        var drag = d3.behavior
            .drag()
            .origin(Object)
            .on("drag", function(d) {
                var np = d3.mouse(this.parentNode);
                table.x += np[0];
                table.y += np[1]
                // TODO: range check
                table.x = Math.max(0, table.x);
                table.y = Math.max(0, table.y);
                table.x = Math.min(table.x, width - table1.boxWidth);
                table.y = Math.min(table.y, height - table1.boxHeight);
                var tableSvg = d3.select(this.parentNode);
                d3.transition(tableSvg).attr('transform', translate(table.x, table.y));
                updateLine();
            });

        var tableG = svg
            .append('g')
            .attr(
            'transform',
            function() { return translate(table.x, table.y); }
        );

        var titleG = tableG.append("g").call(drag);

        var titleBox = titleG.append("svg:rect")
            .style("fill", "GoldenRod")
            .style("fill-opacity", ".4")
            .style("stroke", "#777")
            .attr("width", table.boxWidth)
            .attr("height", table.boxHeight);

        var title = titleG.append("svg:text")
            .style("font-size", fontSize)
            .style("font-family", "monospace")
            .attr("x", fontOffsetX)
            .attr("y", fontOffsetY)
            .text(table.name);

        var tableItems = tableG
            .append("g")
            .selectAll("g")
            .data(table.columns)
            .enter()
            .append('g')
            .attr("transform",
            function(d, i) {
                return translate(0, (i + 1) * table.boxHeight);
            })
            .attr("id",
                function(d, i) {
                    return leftness == false ? "l" + i : "r" + i;
                })
            .on("mouseover",
                function() {
                    d3.select(this)
                        .select("rect")
                        .style("stroke-width", 5)
                        .style("stroke", "green");
                })
            .on("mouseout",
            function() {
                d3.select(this)
                    .select("rect")
                    .style("stroke", "#777")
                    .style("stroke-width", 1);
            })
            .on("click",
                function(d) {
                    if (firstPosition == null) {
                        firstPosition = d3.event;
                        firstNode = this;
                    } else {
                        if (firstNode.parentNode == this.parentNode) {
                            console.log('cannot pick node from same table');
                            firstPosition = null;
                        } else {
                            secondPosition = d3.event;
                            secondNode = this;
                            conns.push([firstNode.id, secondNode.id, 0, 0, 1, conns.length]);
                            updateLine();
                            firstPosition = null;
                            secondPosition = null;
                            firstNode = null;
                            secondNode = null;
                            drawTrace();

                            _.each(onLineAddedHandlers, function(f) {
                                f(d);
                            });
                        }
                    }
                }
            );

        tableItems.append("svg:rect")
            .style("fill", "steelblue")
            .style("fill-opacity", ".4")
            .style("stroke", "#777")
            .style("stroke-width", 1)
            .attr("width", table.boxWidth)
            .attr("height", table.boxHeight);

        tableItems.append("svg:circle")
            .attr("cx", function() { return leftness == true ? -10 : table.boxWidth + 10; })
            .attr("cy", table.boxHeight * 0.5)
            .attr("r", 2)
            .style("fill", "none")
            .style("stroke-width", 1)
            .style("stroke", "#777");

        tableItems.append("svg:text")
            .style("font-size", fontSize)
            .style("font-family", "monospace")
            .attr("x", fontOffsetX)
            .attr("y", fontOffsetY)
            .text(function(d) { return d; });
    }

    function render() {
        width = $('#playground').width();
        height = 600;
        svg = d3.select("#playground")
            .append("svg")
            .attr("width", width)
            .attr("height", height);

        traceSvg = svg.append("g");

        // invisible box following the mouse
        var visg = svg.append("g").attr("class", "g_main");
        visg
            .append("rect")
            .attr("width", width)
            .attr("height", height)
            .style('visibility', 'hidden')
            .on("mousemove", function() {
                drawTrace({
                    "x" : d3.mouse(this)[0],
                    "y" : d3.mouse(this)[1]
                });
            });

        drawTable(table1);
        drawTable(table2);
    }


    function bindEvent() {
        onLineAdded(function(d) {
            updateRule();
        });
    }

    function updateRule() {
        var trs =
            d3.select("#ruleTableBody")
                .selectAll("tr")
                .data(conns);

        trs.exit().remove();
        trs.enter().append("tr");
        trs.html(function (d, i) {
            return _.template(
                TableTemplate, {
                    id: i,
                    left: table1.name + '.' + table1.columns[d[0].substr(1)],
                    right: table2.name + '.' + table2.columns[d[1].substr(1)],
                    op: d[2],
                    cmp: d[3],
                    value: d[4]
                });
        }).each(function(d, i) {
            $("#del" + i).on("click", function () {
                conns.splice(i, 1);
                updateRule();
                updateLine();
            });

            $("#op" + i).change(function () {
                conns[i][2] = $("#op" + i + " option:selected").val();
            });

            $("#cmp" + i).change(function () {
                conns[i][3] = $("#cmp" + i + " option:selected").val();
            });
        });
    }

    function getRule() {
        for (var i = 0; i < conns.length; i ++)
            conns[i][4] = $("#v" + i).val();
        return conns;
    }

    function setRule(d) {
        if (!_.isArray(d))
            return;
        conns = d;
        updateRule();
        updateLine();
    }

    return {
        start : start,
        getRule : getRule,
        setRule : setRule
    }
})
