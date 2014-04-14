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
    'table',
    'jquery.filedrop',
    'state',
    'mvc/CleanPlanView',
    'mvc/ProgressbarView',
    'mvc/SourceEditorView',
    'text!mvc/template/controller.template.html',
    'text!mvc/template/detail.template.html'
], function(
    Requester,
    Table,
    FileDrop,
    State,
    CleanPlanView,
    ProgressBarView,
    SourceEditorView,
    ControllerTemplate,
    DetailTemplate
) {
    var domId;

    function info(msg) {
        $('#home-alert').html([
            ['<div class="alert alert-success" id="home-alert-info">'],
            ['<button type="button" class="close" data-dismiss="alert">'],
            ['&times;</button>'],
            ['<span><h3>' + msg + '</h3></span></div>']].join(''));

        window.setTimeout(function() { $('#home-alert-info').alert('close'); }, 5000);
    }

    function err(msg) {
        $('#home-alert').html([
            ['<div class="alert alert-error">'],
            ['<button type="button" class="close" data-dismiss="alert">'],
            ['&times;</button>'],
            ['<span><h4>' + msg + '</h4></span></div>']].join(''));
    }

    function render(id) {
        domId = id;
        refreshList();
    }

    function getSelectedPlan() {
        return $("#selected_rule").val();
    }

    function getSelectedSource() {
        return $("#selected_source").val();
    }

    function refreshList() {
        Requester.getSource(
            function(source) {
                var sources = source['data'];
                State.set('source', sources);
                Requester.getRule(
                    function(data) {
                        var rules = data['data'];
                        State.set('rule', rules);
                        var html =
                            _.template(ControllerTemplate)
                                ({ sources: sources, plans: []});
                        $('#' + domId).html(html);

                        // render the source modal
                        // TODO: put to on shown
                        SourceEditorView.start('source-editor-modal');
                        bindEvent();
                        ProgressBarView.start('progressbar');
                    }
                );
            }
        );
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

    function deleteRule(rules) {
        _.each(rules, function(rule) {
            Requester.deleteRule(
                rule,
                function() {
                    info("Selected rules are deleted.");
                    refreshList();
                }
            );
        });
    }

    function renderRuleDetail() {
        var selectedPlan = getSelectedPlan();
        if (_.isUndefined(selectedPlan) || _.isNull(selectedPlan))
            return;

        if (_.isArray(selectedPlan) && selectedPlan.length != 1) {
            $('#detail').html('');
        } else {
            Requester.getRuleDetail(
                selectedPlan,
                function (data) {
                    var plan = arrayToPlan(data['data'][0]);
                    $('#detail').html(_.template(DetailTemplate, plan));
            });
        }
    }

    function renderRuleList(source) {
        var ruleList = State.get('rule');
        var selectedRule =
            _.filter(ruleList, function(x) { return x[3] === source || x[4] === source; })
        var selectedHtml = _.template(
            "<% _.each(rules, function(rule) { %>" +
            "<option value='<%= rule[0] %>'><%= rule[0] %></option>" +
            "<% }); %>", { rules : selectedRule });
        $('#selected_rule').html(selectedHtml);
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
                        info("A repair job is successfully submitted.");
                        var key = data['data'];
                        console.log('Received job key : ' + key);
                        if (key != null) {
                            var jobList = State.getJob();
                            jobList.push({ key : rule.name });
                        }
                    },
                    error: function(data, status) {
                        var json = JSON.parse(data.responseText);
                        err("<strong>Error</strong>: " + json['error']);
                    }
                });
            });
        });
    }

    function detect(plans) {
        // clean the violation before start new detection.
        Requester.deleteViolation(
            function() {
                _.each(plans, function(planName) {
                    Requester.getRuleDetail(
                        planName,
                        function(data) {
                            var plan = arrayToPlan(data['data'][0]);
                            Requester.doDetect(
                                plan,
                                function(data) {
                                    info("A job is successfully submitted.");
                                    var key = data['data'];
                                    console.log('Received job key : ' + key);
                                    if (key != null) {
                                        var jobList = State.get("job");
                                        jobList.push({ name : plan.name, key : key });
                                        State.set("job", jobList);
                                    }
                                },

                                function(data) {
                                    var json = JSON.parse(data.responseText);
                                    err("<strong>Error</strong>: " + json['error']);
                                }
                            )
                        }
                     );
                });
            }
        );
    }

    function bindEvent() {
        $('#refresh_source').on('click', function() {
            refreshList();
        });

        $('#new_plan').on('click', function() {
            CleanPlanView.render(
                'cleanPlanView',
                {name: null, type: 'FD', source: null, tablename: null}
            );
        });

        $('#cleanPlanPopup').on('hidden', function() {
            refreshList();
        });

        $('#edit_plan').on('click', function() {
            var selectedPlan = getSelectedPlan();
            if (selectedPlan == null) {
                err('No rule is selected.');
                return;
            }

            if (_.isArray(selectedPlan) && selectedPlan.length != 1) {
                err('Can not edit multiple clean plans.');
                return;
            }

            Requester.getRuleDetail(selectedPlan[0], function(data) {
                var plan = data['data'][0];
                CleanPlanView.render('cleanPlanView', arrayToPlan(plan));
            });
        });

        $('#new-source').on('click', function() {
            $('#source-editor').modal('show');
        });

        $('#selected_rule').on('change', function() {
            renderRuleDetail();
        });

        $('#selected_source').on('change', function() {
            var newSource = getSelectedSource();
            renderRuleList(newSource);
            Table.load({ domId: 'source-table', table: newSource });
        });

        $('#detect').on('click', function() {
            var selectedPlan = getSelectedPlan();
            if (selectedPlan == null || !_.isArray(selectedPlan)) {
                err('No rule is selected.');
                return;
            }

            detect(selectedPlan);
        });

        $('#delete').on('click', function() {
            var selectedPlan = getSelectedPlan();
            if (selectedPlan == null || !_.isArray(selectedPlan)) {
                err('No rule is selected.');
                return;
            }

            deleteRule(selectedPlan);
        });

        $('#repair').on('click', function() {
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
