define(
	[
	 'table',
	 'mvc/CleanPlanView', 
	 'mvc/ProgressbarView',
	 'text!mvc/template/controller.template.html',
	 'text!mvc/template/detail.template.html'],
	function(Table, CleanPlanView, ProgressBarView, ControllerTemplate, DetailTemplate) {
		var isSubscribed;
		var domId;

		function info(msg) {
			$('#home-alert').html([
				['<div class="alert alert-success" id="home-alert-info">'],
				['<button type="button" class="close" data-dismiss="alert">'],
				['&times;</button>'],
				['<span>' + msg + '</span></div>']].join(''));
			
			window.setTimeout(function() { $('#home-alert-info').alert('close'); }, 2000);
		}
        
        function err(msg) {
            $('#home-alert').html([
		        ['<div class="alert alert-error">'],
		        ['<button type="button" class="close" data-dismiss="alert">'],
                ['&times;</button>'],
		        ['<span>' + msg + '</span></div>']].join(''));	
        }

		function render(id) {
			domId = id;
			renderRuleList();
			ProgressBarView.start('progressbar');
		}
        
        function getSelectedPlan() {
	        return $("#selected_rule").val();
        }

        function getSelectedSource() {
	        return $("#selected_source").val();
        }

		function renderRuleList() {
			$.getJSON('/data/source', function(source) {
				var sources = source['data'];
				$.getJSON('/data/rule', function(data) {
					var plans = data['data'];
					var html = _.template(ControllerTemplate)({ sources: sources, plans: plans});
					$('#' + domId).html(html);
					
					bindEvent();
				});
			});
		}

		function arrayToPlan(v) {
			return {
				name: v[0],
				type: v[1],
				code: v[2],
				table1: v[3],
				table2: v[4]
			};
		}

		function renderRuleDetail() {
			var selectedPlan = getSelectedPlan();
			if (_.isArray(selectedPlan) && selectedPlan.length != 1) {
				$('#detail').html('');
			} else {
				$.getJSON('/data/rule/' + selectedPlan, function(data) {
					var plan = arrayToPlan(data['data'][0]);
					var html = 
						_.template(
							DetailTemplate, 
							plan
						);
					$('#detail').html(html);
				});
		    }
		}
		
		function renderTable() {
			var selectedSource = getSelectedSource();
			var activeTable = $('#tables li.active')[0].id;
			if (activeTable == 'tab_source') {
				Table.load(getSelectedSource());
			}
		}

		function repair(rules) {
			_.each(rules, function(ruleName) {
				$.getJSON('/data/rule/' + ruleName, function(data) {
					var rule = arrayToPlan(data['data'][0]);
					$.ajax({
						url : '/do/repair',
						type : 'POST',
						dataType : 'json',
						data: rule,
						success: function(data, status) {
							info("A repair job is successfully submited.");
							var key = data['result'];
							console.log('Received job key : ' + key);
							if (key != null) {
								window.localStorage[key] = rule.name;
							}
						}, 
						error: function(data, status) {
							err("<strong>Error</strong>: " + data.responseText);
						}
					});
				});
			});
		}
		
		function detect(plans) {
			// clean the violation before start new detection.
			$.ajax({
				url: '/table/violation',
				type: "DELETE",
				success: function(e) {
					_.each(plans, function(planName) {
						$.getJSON('/data/rule/' + planName, function(data) {
							var plan = arrayToPlan(data['data'][0]);
							$.ajax({
								url : '/do/detect',
								type : 'POST',
								dataType : 'json',
								data: plan,
								success: function(data, status) {
									info("A job is successfully submited.");
									var key = data['result'];
									console.log('Received job key : ' + key);
									if (key != null) {
										window.localStorage[key] = plan.name;
									}
								}, 
								error: function(data, status) {
									err("<strong>Error</strong>: " + data.responseText);
								}
							});
						});
					});
				}
			});
		}
		
		function bindEvent() {
			$('#refresh_rule').on('click', function(e) {
				renderRuleList();
			});

			$('#refresh_source').on('click', function(e) {
				renderRuleList();
			});
			
			$('#new_plan').on('click', function(e) {
				CleanPlanView.render(
					'cleanPlanView',
					{name: null, type: 'FD', source: null, tablename: null}
				);
			});

			$('#cleanPlanPopup').on('hidden', function(e) {
				renderRuleList();
				// info('You just successfully updated a rule.');				
			});

			$('#edit_plan').on('click', function(e) {
				var selectedPlan = getSelectedPlan();
				if (selectedPlan == null) {
					err('No CleanPlan is selected.');
					return;
				}
				
				if (_.isArray(selectedPlan) && selectedPlan.length != 1) {
					err('Can not edit multiple clean plans.');
					return;
				}

				$.getJSON('/data/rule/' + selectedPlan[0], function(data) {
					var plan = data['data'][0];
					CleanPlanView.render('cleanPlanView', arrayToPlan(plan));
				});				
			});

			$('#selected_rule').on('change', function(e) {
				renderRuleDetail();
			});

			$('#selected_source').on('change', function(e) {
				renderTable();
			});
			
			$('#detect').on('click', function(e) {
				var selectedPlan = getSelectedPlan();
				if (selectedPlan == null || !_.isArray(selectedPlan)) {
					err('No rule is selected.');
					return;
				}
				
				detect(selectedPlan);
			});

			$('repair').on('click', function(e) {
				var selectedPlan = getSelectedPlan();
				if (selectedPlan == null || !_.isArray(selectedPlan)) {
					err('No rule is selected.');
					return;
				}
				
				repair(selectedPlan);				
			});
		}

		return {
			render: render
		};
});
