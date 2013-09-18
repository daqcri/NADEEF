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

$(function() {
	var ruleName = "unknown";
	var progress = 10;
	$("#container").append(
		'<div class="well well-small"><span>' + ruleName + '</span></div>'
	);
	
	var bars = '';
	for (var i = 0; i < 10; i ++) {
		bars += '<span>Unknown</span>';
		bars +=	'<div class="progress progress-striped active"><div class="bar" style="width: ' + 
			progress * i + '%;"></div></div>' ;
	}
    $('#container div').append(bars);
});
