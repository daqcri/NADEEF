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
    "text!mvc/template/editor.template.html",
    "node-editor"
], function(
    EditorTempalte,
    nodeEditor
) {
    var editor;
    var rules = [[]];
    var ruleNames = ["Default"];
    var currentRuleIndex = 0;
    var table1;
    var table2;
    function resizeAce() {
        return $('#ace-editor').height($(window).height());
    }

    function err(msg) {
        $('#node-alert').html([
            ['<div class="alert alert-danger">'],
            ['<button type="button" class="close" data-dismiss="alert">'],
            ['&times;</button>'],
            ['<span>' + msg + '</span></div>']].join(''));
    }

    function render() {
        var homeHtml = _.template(EditorTempalte);
        $("body").html(homeHtml);
        table1 = {
            'name' : 'qatarcars',
            'columns' :
                ['id', 'title', 'url', 'model', 'mileage',
                    'price', 'contact_number', 'description', 'brand_name',
                    'class_name', 'source_id', 'camera', 'sensors', 'location'
                ]
        };

        table2 = {
            'name' : 'qatarcars',
            'columns' :
                ['id', 'title', 'url', 'model', 'mileage',
                    'price', 'contact_number', 'description', 'brand_name',
                    'class_name', 'source_id', 'camera', 'sensors', 'location'
                ]
        };

        nodeEditor.start(table1, table2);

        // trigger extension
        ace.require("ace/ext/language_tools");
        editor = ace.edit("ace-editor");
        editor.session.setMode("ace/mode/java");
        editor.setTheme("ace/theme/tomorrow");
        // enable autocompletion and snippets
        editor.setOptions({
            enableBasicAutocompletion: true,
            enableSnippets: true
        });
        editor.setFontSize(14);
        resizeAce();
    }

    function bindEvent() {
        $(window).resize(resizeAce);

        $("#btn-delete").on("click", function () {
            if (currentRuleIndex == 0) {
                err("Default rule cannot be deleted.");
                return;
            }

            rules.splice(currentRuleIndex, 1);
            ruleNames.splice(currentRuleIndex, 1);
            $("#rule-name-list")[0][currentRuleIndex].remove();
            currentRuleIndex --;
            $("#rule-name-list").val(currentRuleIndex);
            nodeEditor.setRule(rules[currentRuleIndex]);
        });

        $("#btn-reset").on("click", function () {
            nodeEditor.setRule([]);
            rules[currentRuleIndex] = [];
        });

        $("#btn-save-rule").on("click", function () {
            var ruleName = $("#rule-name").val();
            for (var i = 0; i < ruleNames.length; i ++) {
                var d = ruleNames[i];
                if (ruleName == d) {
                    $("#rule-input-group").addClass("has-error");
                    $("#rule-name-feedback").text(ruleName + " is already existed.");
                    return;
                }
            }

            ruleNames.push(ruleName);
            rules.push([]);
            $("#rule-name-list").append(
                "<option value=" + (ruleNames.length - 1) + ">" + ruleName + "</option>"
            );
            rules[currentRuleIndex] = nodeEditor.getRule();
            currentRuleIndex = rules.length - 1;
            nodeEditor.setRule(rules[currentRuleIndex]);
            $("#rule-name-list").val(currentRuleIndex);
            $("#rule-edit-modal").modal('hide');
        });

        $("#rule-name-list").change(function () {
            currentRuleIndex = this.value;
            nodeEditor.setRule(rules[currentRuleIndex]);
        });

        $("#btn-generate").on("click", function () {
            var conns = nodeEditor.getRule();

            if (_.isNull(conns) || _.isEmpty(conns) || conns.length == 0) {
                err("Rule is not defined.");
                return;
            }

            var cmd = "";
            _.each(conns, function(d) {
                cmd += int2op(d[2]);
                cmd += "(";
                cmd += table1.name + "." + table1.columns[d[0].substr(1)];
                cmd += ",";
                cmd += table2.name + "." + table2.columns[d[1].substr(1)];
                cmd += ")";
                cmd += int2cmp(parseInt(d[3]));
                cmd += d[4];
                cmd += "\n";
            });

            var ruleName = $("#rule-name-list option:selected").text();

            var rule = {
                type: "er",
                name: ruleName,
                code: cmd,
                table1: table1.name,
                table2: table2.name,
                project: "unittest"
            };

            $.ajax({
                url: "http://localhost:4567/do/generate",
                type: "POST",
                data: rule,
                success: function (result) {
                    var code = result['data'];
                    if (!_.isNull(code)) {
                        editor.setValue(code, -1);
                    }
                },
                fail: function(result) {
                    console.log("failed");
                }
            })
        });
    }

    function int2op(x) {
        var op;
        switch(x) {
            default:
            case 0:
                op = 'EQ'; break;
            case 1:
                op = 'ED'; break;
            case 2:
                op = 'LS'; break;
            case 3:
                op = 'QG'; break;
            case 4:
                op = 'SD'; break;
        }
        return op;
    }

    function int2cmp(x) {
        var cop;
        switch(x) {
            default:
            case 0:
                cop = '='; break;
            case 1:
                cop = '>'; break;
            case 2:
                cop = '<'; break;
            case 3:
                cop = '>='; break;
            case 4:
                cop = '<='; break;
            case 5:
                cop = '!='; break;
        }
        return cop;
    }

    function start() {
        render();
        bindEvent();
    }

    return {
        start: start
    };
})