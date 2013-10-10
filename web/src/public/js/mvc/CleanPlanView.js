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
    'ace',
    'text!mvc/template/cleanplan.template.html'
], function(Requester, Ace, CleanPlanTemplate) {
        var editor;
        var sources;
        var rules;

        function getRule() {
            var type =  $('#select_ruletype').find('.active')[0].id;
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
            Requester.getSource(function(data) {
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
                editor = Ace.edit("rule-editor");
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
                Requester.createRule(
                    rule,
                    function() {
                        $('#cleanPlanPopup').modal('hide');

                    },
                    function(data) {
                        err("<strong>Error</strong>: " + data.responseText);
                    }
                );
            });

            $('#generate').on('click', function(e) {
                var rule = getRule();
                Requester.doGenerate(
                    rule,
                    function(data) {
                        var code = data['data'];
                        if (!code) {
                            err("<string>Error</string>: Code generation failed.");
                        } else {
                            editor.setValue(code, -1);
                        }
                    },
                    function(data) {
                        err("<strong>Error</strong>: " + data.responseText);
                    }
                );
            });

            $('#verify').on('click', function(e) {
                var rule = getRule();
                Requester.doVerify(
                    rule,
                    function(data, status) {
                        var result = data['data'];
                        if (result) {
                            info("Verification succeeded.");
                        } else {
                            err("Verification failed");
                        }
                    },
                    function(data, status) {
                        err("<strong>Error</strong>: " + data.responseText);
                    }
                );
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
