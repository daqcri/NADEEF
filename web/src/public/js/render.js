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
define([
    'hash', 'd3', 'nvd3', 'table', 'requester'],
    function(HashMap, D3, NVD3, Table, Requester) {
    function drawOverview(id) {
	    Requester.getOverview(function(json) {
			$('#' + id + ' svg').empty();
			
		    var data = json['data'];
		    // TODO: fix this hack
		    var clean = data[0];
		    var polluted = data[1];
		    var values;
		    if (clean == 0 && polluted == 0) {
			    values = [];
		    } else {
			    values = [
				    {"label": "Clean Tuples", "value" : clean},
				    {"label": "Dirty Tuples", "value" : polluted}
			    ];
		    }

		    var pie_data = [{
			    key: "Cumulative Return",
			    values: values
		    }];

		    nv.addGraph(function() {
			    var chart = nv.models.pieChart()
				    .x(function(d) { return d.label })
				    .y(function(d) { return d.value })
				    .showLabels(true);

			    d3.select("#" + id + " svg")
				    .datum(pie_data)
				    .transition().duration(500)
				    .call(chart);
			    return chart;
		    });
	    });
    }

    function drawAttribute(id) {
	    Requester.getAttribute(function(json) {
			$('#' + id + ' svg').empty();
		    var result = json['data'];
		    var values;
		    if (result == null && result.length == 0) {
			    values = null;
		    } else {
			    values = [];
			    for (var i = 0; i < result.length; i ++) {
				    values.push(
				        {'label' : result[i][0], 'value' : result[i][1]}
				    );
			    }
		    }

		    var graph_data = [{
			    key: "Cumulative Return",
			    values: values
		    }];

		    nv.addGraph(function() {
			    var chart = nv.models.discreteBarChart()
				    .x(function(d) { return d.label })
				    .y(function(d) { return d.value })
				    .staggerLabels(false)
				    .tooltips(true)
				    .showValues(true);

			    d3.select("#" + id + " svg")
				    .datum(graph_data)
				    .transition().duration(500)
				    .call(chart);

			    nv.utils.windowResize(chart.update);

			    chart.discretebar.dispatch.on('elementClick', function(e) {
				    console.log(e.point.label);
				    Table.filter(e.point.label);
			    });
			    return chart;
		    });
	    });
    }

    function drawViolationRelation(id) {		
	    Requester.getViolationRelation(function(data) {
			var width = $('#violationRelation').width();
			var height = $('#violationRelation').height();
			
			$("#" + id + " svg").empty();			
		    var result = data['data'];
			// TODO: find a better pattern
			if (result.length == 1 && result[0] == -1) {
				d3.select("#" + id + " svg")
					.append("text")
				    .attr("x", 200)
				    .attr("y", 150)
				    .style("font-size", "20px")
				    .text("There are too many violations to show.");
				return;
			}
		    var links = [];
		    var nodes = [];

		    var vid;
		    var tid;
		    var groupId;
		    var maxGroupId = 0;
		    var pre = null;
		    var conns = [];
			var hash = new HashMap.Map();

		    // process the graph
		    for (var i = 0; i < result.length; i ++) {
			    vid = result[i][0];
			    tid = result[i][1];
			    if (hash.hasKey(tid)) {
				    groupId = hash.get(tid);
			    } else {
				    groupId = maxGroupId;
				    hash.put(tid, maxGroupId);
				    nodes.push({'name' : tid, 'group' : groupId});
				    maxGroupId ++;
			    }

			    if (vid === pre) {
				    for (var j = 0; j < conns.length; j ++) {
					    var gid = hash.get(conns[j]);
					    // TODO: use different value
					    links.push(
						    {'source' : gid, 
						     'target' :  groupId, 
						     'value' : 1,
						     'weight' : 1
						    }
						);
				    }
				    conns.push(tid);
			    } else {
				    conns = [];
				    conns.push(tid);
				    pre = vid;
			    }
		    }

		    nv.addGraph(function() {
			    var color = d3.scale.category20();

			    var svg = d3.select("#" + id + " svg")
    			    .attr("pointer-events", "all")
  				    .append('svg:g')
    			    .call(d3.behavior.zoom().on("zoom", function() {
					    svg.attr(
						    "transform",
					        "translate(" + d3.event.translate + ")" + 
					            " scale(" + d3.event.scale + ")"
					    );
				    }));	
			    
			    var force = d3.layout.force()
				    .charge(-200)
				    .linkDistance(150)
				    .size([width, height])
				    .nodes(nodes)
				    .links(links)
				    .start();

			    var link = svg.selectAll(".link")
				    .data(links)
				    .enter().append("line")
				    .attr("class", "link")
				    .style("stroke-width", function(d) { return Math.sqrt(d.value); });

			    var node = svg.selectAll(".node")
				    .data(nodes)
				    .enter().append("circle")
				    .attr("class", "node")
				    .attr("r", 15)
				    .style("fill", function(d) { return color(d.group * 2); })
				    .call(force.drag);

			    node.append("title")
				    .text(function(d) { return d.name; });

			    force.on("tick", function() {
				    link.attr("x1", function(d) { return d.source.x; })
					    .attr("y1", function(d) { return d.source.y; })
					    .attr("x2", function(d) { return d.target.x; })
					    .attr("y2", function(d) { return d.target.y; });

				    node.attr("cx", function(d) { return d.x; })
					    .attr("cy", function(d) { return d.y; });
			    });

			    svg.selectAll('circle.node').on('click', function(e) {
				    Table.filter(e.name);
			    });
		    });
	    });
    }

    function drawDistribution(id) {
	    Requester.getRuleDistribution(function(data) {
			$('#' + id + '  svg').empty();
		    var result = data['data'];
		    if (result == null || result.length == 0) {
			    return;
		    }

		    var affectedTuple = [];
		    var affectedTable = [];
		    for (var i = 0; i < result.length; i ++) {
			    affectedTuple.push({'label' : result[i][0], 'value' : result[i][1]});
			    affectedTable.push({'label' : result[i][0], 'value' : result[i][2]});
		    }

		    var distribution_data = [
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

		    nv.addGraph(function() {
			    var chart = nv.models.multiBarHorizontalChart()
				    .x(function(d) { return d.label })
				    .y(function(d) { return d.value })
				    .margin({top: 30, right: 20, bottom: 50, left: 175})
				    .showValues(true)
				    .tooltips(false)
				    .showControls(false);

			    chart.yAxis
				    .tickFormat(d3.format(',.2f'));

			    d3.select("#" + id + " svg")
				    .datum(distribution_data)
				    .transition().duration(500)
				    .call(chart);

			    nv.utils.windowResize(chart.update);

			    chart.dispatch.on(
				    'stateChange',
                    function(e) { nv.log('New State:', JSON.stringify(e)); }
			    );

			    return chart;
		    });
	    });
    }
    
    function drawTupleRank(id) {
	    Requester.getTupleRank(function(data) {
			$('#' + id + ' svg').empty();
		    var result = data['data'];
		    var values = [];
		    for (var i = 0; i < result.length; i ++) {
			    values.push({'label' : result[i][0], 'value' : result[i][1]});
		    }
		    
		    var graph_data = [{
			    key: "Tuple Rank",
			    values: values
		    }];

		    nv.addGraph(function() {
			    var chart = nv.models.discreteBarChart()
				    .x(function(d) { return d.label })
				    .y(function(d) { return d.value })
				    .staggerLabels(false)
				    .tooltips(true)
				    .showValues(true);

			    d3.select("#" + id + " svg")
				    .attr("width", 400)
				    .attr("height", 300)
				    .datum(graph_data)
				    .transition().duration(500)
				    .call(chart);

			    nv.utils.windowResize(chart.update);

			    chart.discretebar.dispatch.on('elementClick', function(e) {
				    Table.filter(e.point.label);
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
