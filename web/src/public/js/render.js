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

/**
 * Render module which draws different visualization graphs.
 */
define(['hash', 'nvd3', 'table', 'requester'], function (HashMap, NVD3, Table, Requester) {
    "use strict";
    function drawOverview(id) {
        Requester.getOverview(function (json) {
            $('#' + id + ' svg').empty();

            var data = json.data;
            // TODO: fix this hack
            var clean = data[0];
            var polluted = data[1];
            var values;
            if (clean === 0 && polluted === 0) {
                values = [];
            } else {
                var sum = clean + polluted;
                var cleanPerc = Math.round((clean / sum) * 100);
                var pollutedPerc = 100 - cleanPerc;

                values = [
                    {"label": "Clean Tuples " + cleanPerc + "%", "value" : clean},
                    {"label": "Dirty Tuples " + pollutedPerc + "%", "value" : polluted}
                ];
            }

            nv.addGraph(function () {
                var chart = nv.models.pieChart()
                    .x(function (d) { return d.label; })
                    .y(function (d) { return d.value; })
                    .color(d3.scale.category10().range())
                    .showLabels(true)
                    .valueFormat(d3.format('d'));

                d3.select("#" + id + " svg")
                    .datum(values)
                    .transition(10)
                    .call(chart);
                return chart;
            });
        });
    }

    function drawAttribute(id) {
        Requester.getAttribute({
            success: function (json) {
                $('#' + id + ' svg').empty();
                var result = json.data;
                var values;
                if (result == null && result.length === 0) {
                    values = null;
                } else {
                    values = [];
                    for (var i = 0; i < result.length; i++) {
                        values.push(
                            {'label': result[i][0], 'value': result[i][1]}
                        );
                    }
                }

                var graphData = [{
                    key: "Cumulative Return",
                    values: values
                }];

                nv.addGraph(function () {
                    var chart = nv.models.discreteBarChart()
                        .x(function (d) {
                            return d.label;
                        })
                        .y(function (d) {
                            return d.value;
                        })
                        .staggerLabels(false)
                        .tooltips(true)
                        .showValues(true)
                        .valueFormat(d3.format('d'));

                    chart.xAxis.axisLabel("Attribute");
                    chart.yAxis
                        .axisLabel("Number of Violation")
                        .axisLabelDistance(50)
                        .tickFormat(d3.format('d'));
                    d3.select("#" + id + " svg")
                        .datum(graphData)
                        .transition().duration(10)
                        .call(chart);

                    nv.utils.windowResize(chart.update);

                    chart.discretebar.dispatch.on('elementClick', function (e) {
                        console.log(e.point.label);
                        Table.filter(e.point.label);
                    });
                    return chart;
                });
            }
        });
    }

    function drawViolationRelation(id) {
        Requester.getViolationRelation({
            success: function (data) {
                var width = $('#violationRelation').width();
                var height = $('#violationRelation').height();

                $("#" + id + " svg").empty();
                var result = data.data;
                // TODO: find a better pattern
                if (data.code === 2) {
                    d3.select("#" + id + " svg")
                        .append("text")
                        .attr("x", width / 2 - 80)
                        .attr("y", height / 2 - 50)
                        .style("font-size", "20px")
                        .style("font-weight", "bold")
                        .text("There are too many violations to show.");
                    return;
                }

                if (data.data && data.data.length === 0) {
                    d3.select("#" + id + " svg")
                        .append("text")
                        .attr("x", width / 2 - 80)
                        .attr("y", height / 2 - 50)
                        .style("font-size", "20px")
                        .style("font-weight", "bold")
                        .text("No Data Available.");
                    return;
                }

                var i,
                    count = 0,
                    links = [],
                    hash = {},
                    nodes = [];

                // cluster the graph
                for (i = 0; i < result.length; i += 2) {
                    var tid0 = "tid_" + result[i][1];
                    var tid1 = "tid_" + result[i + 1][1];
                    var node0, node1;
                    if (tid0 in hash) {
                        // add to existing cluster
                        node0 = hash[tid0];
                    } else {
                        node0 = count;
                        hash[tid0] = count;
                        nodes.push({'name': tid0, 'group': count, 'tid': result[i][1] });
                        count ++;
                    }

                    if (tid1 in hash) {
                        // add to existing cluster
                        node1 = hash[tid1];
                    } else {
                        node1 = count;
                        hash[tid1] = count;
                        nodes.push({'name': tid1, 'group': count, 'tid': result[i + 1][1] });
                        count ++;
                    }

                    links.push({
                        'source': node0,
                        'target': node1,
                        'value': 1,
                        'weight': 1
                    });
                }

                nv.addGraph(function () {
                    var color = d3.scale.category20();
                    var svg = d3.select("#" + id + " svg")
                        .attr("pointer-events", "all")
                        .append('svg:g')
                        .call(d3.behavior.zoom().on("zoom", function () {
                            svg.attr(
                                "transform",
                                    "translate(" + d3.event.translate + ")" +
                                    " scale(" + d3.event.scale + ")"
                            );
                        }));

                    var force = d3.layout.force()
                        .charge(-250)
                        .linkDistance(200)
                        .size([width, height])
                        .nodes(nodes)
                        .links(links)
                        .start();

                    var link = svg.selectAll(".link")
                        .data(links)
                        .enter().append("line")
                        .attr("class", "link")
                        .style("stroke-width", function (d) {
                            return 2.0;
                        });

                    var node = svg.selectAll(".node")
                        .data(nodes)
                        .enter().append("circle")
                        .attr("class", "node")
                        .attr("r", 15)
                        .style("fill", function (d) {
                            return color(d.group % 20);
                        })
                        .call(force.drag);

                    node.append("title")
                        .text(function (d) {
                            return d.tid;
                        });


                    force.on("tick", function () {
                        link.attr("x1", function (d) {
                            return d.source.x;
                        }).attr("y1", function (d) {
                            return d.source.y;
                        }).attr("x2", function (d) {
                            return d.target.x;
                        }).attr("y2", function (d) {
                            return d.target.y;
                        });

                        node.attr("cx", function (d) {
                            return d.x;
                        })
                        .attr("cy", function (d) {
                            return d.y;
                        });
                    });

                    svg.selectAll('circle.node').on('click', function (e) {
                        Table.filter("?=" + e.name);
                    });
                });
            }
        });
    }

    function drawDistribution(id) {
        Requester.getRuleDistribution(function (data) {
            $('#' + id + '  svg').empty();
            var result = data.data;
            if (result == null || result.length === 0) {
                return;
            }

            var affectedTuple = [];
            var affectedTable = [];
            for (var i = 0; i < result.length; i ++) {
                affectedTuple.push({'label' : result[i][0], 'value' : result[i][1]});
                affectedTable.push({'label' : result[i][0], 'value' : result[i][2]});
            }

            var distributionData = [
                {
                    key: 'Affected Tuple',
                    color: '#d62728',
                    values: affectedTuple
                },
                {
                    key: 'Affected Table',
                    color: '#1f77b4',
                    values: affectedTable
                }
            ];

            nv.addGraph(function () {
                var chart = nv.models.multiBarHorizontalChart()
                    .x(function (d) { return d.label; })
                    .y(function (d) { return d.value; })
                    .showValues(true)
                    .tooltips(true)
                    .showControls(false)
                    .valueFormat(d3.format('d'));

                chart.yAxis
                    .tickFormat(d3.format('d'))
                    .axisLabel("Number of violation");

                d3.select("#" + id + " svg")
                    .datum(distributionData)
                    .transition(10)
                    .call(chart);

                nv.utils.windowResize(chart.update);

                chart.dispatch.on(
                    'stateChange',
                    function (e) { nv.log('New State:', JSON.stringify(e)); }
                );

                return chart;
            });
        });
    }

    function drawTupleRank(id) {
        Requester.getTupleRank(function (data) {
            $('#' + id + ' svg').empty();
            var result = data.data;
            var values = [];
            for (var i = 0; i < result.length; i ++) {
                values.push({'label' : result[i][0], 'value' : result[i][1]});
            }

            var graphData = [{
                key: "Tuple Rank",
                values: values
            }];

            nv.addGraph(function () {
                var chart = nv.models.discreteBarChart()
                    .x(function (d) { return d.label; })
                    .y(function (d) { return d.value; })
                    .staggerLabels(false)
                    .tooltips(true)
                    .showValues(true)
                    .valueFormat(d3.format('d'));

                chart.yAxis
                    .tickFormat(d3.format('d'))
                    .axisLabelDistance(50)
                    .axisLabel("Number of violation");

                chart.xAxis
                    .axisLabel("Tuple Id in violation rank");

                d3.select("#" + id + " svg")
                    .attr("width", 400)
                    .attr("height", 300)
                    .datum(graphData)
                    .transition().duration(10)
                    .call(chart);

                nv.utils.windowResize(chart.update);

                chart.discretebar.dispatch.on('elementClick', function (e) {
                    Table.filter("?=" + e.point.label);
                });
                return chart;
            });
        });
    }

    return {
        drawOverview : drawOverview,
        drawAttribute : drawAttribute,
        drawDistribution : drawDistribution,
        drawTupleRank : drawTupleRank,
        drawViolationRelation : drawViolationRelation
    };
});
