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
