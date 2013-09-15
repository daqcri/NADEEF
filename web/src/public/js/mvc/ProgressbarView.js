define(
	['text!mvc/template/progressbar.template.html'], 
	function(ProgressBarTemplate) {
		var isSubscribed = false;
		function start(id) {
			if (!isSubscribed) {
				setInterval(function() {
					$.getJSON('/data/progress', function(data) {
						var values = new Array();
						_.each(data['data'], function(v) {
							var name = 
								localStorage[v['key']] ? 
                                localStorage[v['key']] : 'Unknown';
							values.push({ 
								name : name,
								value : v['overallProgress'] 
							});
						});
					    var html = 
                            _.template(ProgressBarTemplate)({ progress: values });
						$('#' + id).html(html);
					});
				}, 1000);
				isSubscribed = true;
			}
		}

		return {
			start: start
		};
	}
);
