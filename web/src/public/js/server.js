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
 * Server class which communicates with the back-end for pulling data.
 */
define(['jquery', function() {
	function start() {
		setInterval(update, 1000);
	}

    function loadRule(rulename) {
		$.getJSON('/data/rule/' + ruleName, function(data) {
			localStorage.currentRule = data['data'];
		});
    }
    
	function update() {
        $.getJSON('/data/source', function(data) {
            localStorage.source = data['data'];
        });
                  
        $.getJSON('/data/rule', 
			var result = data['data'];
			var name = result['rid'];
			var type = result['type'];
			var code = result['code'];
			var tablename = result['tablename'];

			switch(type) {
			case 0:
				$("#tab_rule_udf_java").trigger("click");
				break;
			case 1:
				$("#tab_rule_fd").trigger("click");
				break;
			case 2:
				$("#tab_rule_cfd").trigger("click");
				break;
			}
			editor.setValue(code);

			$("#select_source").selectedIndex = 0;

	}
});


server.loadProgress = function() {
	$.getJSON('/data/progress', function(data) {
		$("#tab_content_progress").empty();
		
		var progressList = data['data'];
		for (var i = 0; i < progressList.length; i ++) {
			var item = progressList[i];
			var key = item['key'];
			var ruleName = server.store.getRuleName(key);
			if (ruleName == null) {
				ruleName = "Unknown";
			}
			var progress = item['progress'];

			$("#tab_content_progress").
				append(
					'<div class="well well-small"><span>' + 
					ruleName + 
					'</span><div class="progress"><div class="bar" style="width: ' + 
					progress + '%;"></div></div>'
				);
		}
	});
}

server.detect = function() {
	var code = Model.getCode();
	var name = Model.getRuleName();
	var type = Model.getRuleType();
	var tablename = Model.getTableName();

	if (Tools.isNullOrEmpty(name)) {
		alert('rule name cannot be null');
		return;
	}

	if (name instanceof Array) {
		name = name[0];
	}

	$.ajax({
		url: "/do/detect",
		type: "POST",
		dataType: "json",
		data: {
			"name" : name,
			"code" : code,
			"type" : type,
			"tablename" : tablename
		},
		success: function(data, status) {
			server.alert(
			    "A detection job is submited. Check the progress tab on how things are going.", 
				'success'
			);
			key = data['result'];
			if (key != null) {
				server.store.addKeyValuePair(key, name);
			} else {
				console.log("Returned key is null");
			}			
		},
		error: function(data, status) {
			server.alert("<strong>Error</strong>: " + data.responseText, 'error');
		}
	});
}

server.repair = function() {
}

server.generate = function() {
	var code = Model.getCode();
	var name = Model.getRuleName();
	var type = Model.getRuleType();
	var tablename = Model.getTableName();

	if (Tools.isNullOrEmpty(name)) {
		alert('rule name cannot be null');
		return;
	}

	if (name instanceof Array) {
		name = name[0];
	}

	$.ajax({
		url: "/do/generate",
		type: "POST",
		dataType: "json",
		data: {
			"name" : name,
			"code" : code,
			"type" : type,
			"tablename" : tablename
		},
		success: function(data, status) {
			code = data['result'];
			editor.setValue(code);
		},
		error: function(data, status) {
			server.alert("<strong>Error</strong>: " + data.responseText, 'error');
		}
	});	
}

server.alert = function(msg, type) {
	var tag;
	switch (type) {
	case 'success':
		tag = "alert-success";
		break;
	case 'error':
		tag = 'alert-error';
		break;
	}
	
    $('#alert-placeholder').html([
		['<div class="alert ' + tag + '">'],
		['<button type="button" class="close" data-dismiss="alert">&times;</button>'],
		['<span>' + msg + '</span></div>']].join(''));	
}
