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
    'mvc/editor/EREditor',
    'text!mvc/template/ruleeditor.template.html'
], function(
    Requester,
    State,
    Ace,
    EREditor,
    CleanPlanTemplate
) {
    var editor;
    var editorView;

    function getRule() {
        var type = $("#rule-type").val(),
            table1 = $('#table1').val(),
            table2 = $("#table2").val(),
            ruleName = $('#rule-name').val();

        if (_.isNull(ruleName) || _.isEmpty(ruleName)) {
            err({ error : "Rule name cannot be empty."});
            return null;
        }

        if (_.isEmpty(table1)) {
            err({ error: 'No table is selected'});
            return null;
        }

        var code = editorView !== null ? editorView.val() : editor.getValue();
        if (_.isNull(code) || _.isEmpty(code)) {
            err({ error : "No content is found, you need to create something."});
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

    function renderEditor() {
        var type = $("#rule-type").val(),
            table1 = $('#table1').val(),
            table2 = $("#table2").val();

        switch (type) {
            case 'FD':
                editorView = EREditor;
                break;
            case 'UDF':
                break;
            case 'DC':
                break;
            case 'ER':
                editorView = EREditor;
                break;
        }

        editorView.render('#advance-editor', table1, table2);
    }

    function render(rule) {
        var dom = $('#rule-editor-modal');
        var sources = State.get("source");
        var cleanPlanHtml =
            _.template(CleanPlanTemplate) ({
                name: rule.name,
                sources : sources,
                table1: rule.table1,
                table2: rule.table2,
                type: rule.type
            });
        dom.html(cleanPlanHtml);

        bindEvent();

        dom.one('shown', function() {
            // render the graphical editor
            // 138 is the space for header and footer, and padding of the body.
            var height = $(this).height() - 138;
            renderEditor(rule);
            $(this).find('.modal-body').css({ height: height + 'px'});

            // initialize the code editor
            editor = Ace.edit("ace-editor");
            $('#ace-editor').css({ height: (height - 100) + 'px'});
            editor.setFontSize(14);
            if (rule.code == null) {
                editor.setValue("// Type your rule code here", -1);
            } else
                editor.setValue(rule.code, -1);
            editor.session.setMode("ace/mode/java");
        });

        dom.modal();
    }

    function bindEvent() {
        $('#save').on('click', function() {
            var rule = getRule();
            if (!_.isNull(rule)) {
                Requester.createRule(rule, {
                    success: function() {
                        $('#cleanPlanModal').modal('hide');
                        info("You have successfully created a rule.");
                    },
                    failure: function(data) { err(data.responseText); }
                });
            }
        });

        $('#rule-type').change(function() {
            renderEditor();
        });

        $('#generate').on('click', function(e) {
            var rule = getRule();
            if (!_.isNull(rule)) {
                Requester.doGenerate(rule, {
                    success: function(data) {
                        info("Code generation succeeded.");
                        editor.setValue(data['data'], -1);
                        $('#rule-editor-tab a[href="#code-editor"]').tab("show");
                    },
                    failure: function(data) { err(data.responseText); }
                });
            }
        });

        $('#verify').on('click', function(e) {
            var rule = getRule();
            if (!_.isNull(rule)) {
                rule.code = editor.getValue();
                if (!_.isNull(code) || _.isEmpty(code)) {
                    err("No Java code is found in the editor.");
                    return;
                }

                Requester.doGenerate(rule, {
                    success: function() { info("Verification succeeded."); },
                    failure: function(data) { err(data.responseText); }
                });
            }
        });

        $('#table1').change(function() { renderEditor(); });
        $('#table2').change(function() { renderEditor(); });
    }

    function info(msg) {
        $('#cleanPlanView-alert-info').alert('close');
        $('#cleanPlanView-alert').html([
            ['<div class="alert alert-success" id="cleanPlanView-alert-info">'],
            ['<button type="button" class="close" data-dismiss="alert">'],
            ['&times;</button>'],
            ['<span><h4>' + msg + '</h4></span></div>']].join(''));
        window.setTimeout(function() { $('#cleanPlanView-alert-info').alert('close'); }, 3000);
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
