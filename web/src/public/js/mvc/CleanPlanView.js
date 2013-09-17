define(
	['ace',
	 'text!mvc/template/cleanplan.template.html'],
	function(Ace, CleanPlanTemplate) {
		var editor;
        var sources;
        var rules;

		function getRule() {
            var type =  $('#select_ruletype .active')[0].id;
			var table1;
			var table2;
			var tables = $('#select_source').val();
			if (tables == null || tables.length == 0) {
				err('No table is selected');
				return;
			} else if (tables.length == 1) {
				table1 = tables[0];
				table2 = '';
			} else if (tables.length == 2) {
				table1 = tables[0];
				table2 = tables[1];
			} else {
				err("We don't support rules with more than 2 tables yet");
				return;
			}

			return {
				code : editor.getValue(),
				name : $('#rule_name').val(),
				type : type,
				table1: table1,
				table2: table2
			};
		}
        
		function render(id, rule) {
            // TODO: use a better chain pattern
            $.getJSON('/data/source', function(data) {
                sources = data['data'];
			    var cleanPlanHtml = 
                    _.template(CleanPlanTemplate) (
						{
							name: rule.name,
							sources : sources,
							table1: rule.table1,
							table2: rule.table2,
							type: rule.type
						});
			    $('#' + id).html(cleanPlanHtml);

                bindEvent();

                // initialize the editor
			    editor = Ace.edit("rule_editor");
			    editor.setFontSize(14);
				if (rule.code == null) {
					editor.setValue("// Type your rule code here", -1);
				} else {
					editor.setValue(rule.code, -1);
				}
			    editor.getSession().setMode("ace/mode/java");
				$('#cleanPlanPopup').modal();
            });
		}

        function bindEvent() {
            $('#save').on('click', function(e) {
				var rule = getRule();
				$.ajax({
					url: "/data/rule",
					type: "POST",
					dataType: "json",
					data: rule,
					success: function(data, status) {						
						$('#cleanPlanPopup').modal('hide');
					},
					error: function(data, status) {
						err("<strong>Error</strong>: " + data.responseText);
					}
				});
            });

			$('#generate').on('click', function(e) {
				var rule = getRule();
				$.ajax({
					url: "/do/generate",
					type: "POST",
					dataType: "json",
					data: rule,
					success: function(data, status) {
						var code = data['data'];
						editor.setValue(code);
						$('#udf').trigger('click');
						
					},
					error: function(data, status) {
						err("<strong>Error</strong>: " + data.responseText);
					}
				});	
			});

			$('#verify').on('click', function(e) {
				var rule = getRule();
				$.ajax({
					url: "/do/verify",
					type: "POST",
					dataType: "json",
					data: rule,
					success: function(data, status) {
						info("Verification successed.", 'success');
					},
					error: function(data, status) {
						err("<strong>Error</strong>: " + data.responseText, 'error');
					}
				});				
			});
        }
        
        function info(msg) {
            $('#cleanPlanView-alert').html([
		        ['<div class="alert alert-success" id="cleanPlanView-alert-info>'],
		        ['<button type="button" class="close" data-dismiss="alert">'],
                ['&times;</button>'],
		        ['<span>' + msg + '</span></div>']].join(''));
			window.setTimeout(function() { $('#cleanPlanView-alert-info').alert('close'); }, 2000);
        }
        
        function err(msg) {
            $('#cleanPlanView-alert').html([
		        ['<div class="alert alert-error">'],
		        ['<button type="button" class="close" data-dismiss="alert">'],
                ['&times;</button>'],
		        ['<span>' + msg + '</span></div>']].join(''));	
        }

		return {
			render: render
		};
});
