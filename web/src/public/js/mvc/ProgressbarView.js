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

define([
    'state',
    'text!mvc/template/progressbar.template.html'],
    function(State, ProgressBarTemplate) {
		var isSubscribed = false;

        function info(msg) {
            $('#home-alert').html([
                ['<div class="alert alert-success" id="home-alert-info">'],
                ['<button type="button" class="close" data-dismiss="alert">'],
                ['&times;</button>'],
                ['<span><h3>' + msg + '</h3></span></div>']].join(''));

            window.setTimeout(function() { $('#home-alert-info').alert('close'); }, 2000);
        }

        function updateProgress(id) {
            $.getJSON('/progress', function(data) {
                var values = [];
                var jobList = State.getJob();
                if (jobList && jobList.length > data['data'].length)
                    info("There are Job(s) finished, please do refresh to see the result.");
                jobList = data['data'];
                State.setJob(jobList);

                _.each(jobList, function(job) {
                    var name = job['key'] ? job['key'] : 'Unknown';
                    values.push({
                        name : name,
                        value : job['overallProgress']
                    });
                });
                var html =
                    _.template(ProgressBarTemplate)({ progress: values });
                $('#' + id).html(html);
            }).fail(function(request, status, err) {
                console.log("Requesting progress failed : " + request.responseText);
            });
        }

		function start(id) {
			if (!isSubscribed) {
                updateProgress(id);
				setInterval(function() {updateProgress(id)}, 5000);
				isSubscribed = true;
			}
		}

		return {
			start: start
		};
	}
);
