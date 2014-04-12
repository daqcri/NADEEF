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
    'requester',
    'state',
    'ace',
    'text!mvc/template/cleanplan.template.html'
], function(Requester, State, Ace, CleanPlanTemplate) {
    var editor;
    var rules;

    function getRule() {
        var type =  $('#select_ruletype').find('.active')[0].id;
        var table1;
        var table2;
        var tables = $('#select_source').val();
        if (_.isNull(tables) || tables.length == 0) {
            err({ error: 'No table is selected'});
            return null;
        } else if (tables.length == 1) {
            table1 = tables[0];
            table2 = '';
        } else if (tables.length == 2) {
            table1 = tables[0];
            table2 = tables[1];
        } else {
            err({ error : "We don't support rules with more than 2 tables."});
            return null;
        }

        var ruleName = $('rule_name').val();
        if (_.isNull(ruleName) || _.isEmpty(ruleName)) {
            err({ error : "Rule name cannot be empty."});
            return null;
        }

        var code = editor.getValue();
        if (_.isNull(code) || _.isEmpty(code)) {
            err({ error : "Code cannot be empty."});
            return null;
        }

        return {
            code : code,
            name : ruleName,
            type : type,
            table1: table1,
            table2: table2
        };
    }

    function render(id, rule) {
        var sources = State.getSource();
        var cleanPlanHtml =
            _.template(CleanPlanTemplate) ({
                name: rule.name,
                sources : sources,
                table1: rule.table1,
                table2: rule.table2,
                type: rule.type
            });
        $('#' + id).html(cleanPlanHtml);

        bindEvent();

        // initialize the editor
        editor = Ace.edit("rule-editor");
        editor.setFontSize(14);
        if (rule.code == null) {
            editor.setValue("// Type your rule code here", -1);
        } else
            editor.setValue(rule.code, -1);
        editor.getSession().setMode("ace/mode/java");

        $('#cleanPlanPopup').modal();
    }

    function bindEvent() {
        $('#save').on('click', function(e) {
            var rule = getRule();
            if (rule != null) {
                Requester.createRule(
                    rule,
                    function() {
                        $('#cleanPlanPopup').modal('hide');
                        info("You have successfully created a rule.");
                    }, function(data) { err(data.responseText); }
                );
            }
        });

        $('#generate').on('click', function(e) {
            var rule = getRule();
            if (!_.isNull(rule)) {
                Requester.doGenerate(
                    rule,
                    function(data) { editor.setValue(data['data'], -1); },
                    function(data) { err(data.responseText); }
                );
            }
        });

        $('#verify').on('click', function(e) {
            var rule = getRule();
            if (!_.isNull(rule)) {
                Requester.doVerify(
                    rule,
                    function() { info("Verification succeeded."); },
                    function(data) { err(data.responseText); }
                );
            }
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
        var json = _.isString(msg) ? JSON.parse(msg) : msg;
        $('#cleanPlanView-alert').html([
            ['<div class="alert alert-error">'],
            ['<button type="button" class="close" data-dismiss="alert">'],
            ['&times;</button>'],
            ['<span><h4>' + json["error"] + '</h4></span></div>']].join(''));
    }

    return {
        render: render
    };
});
