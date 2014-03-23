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
    'mvc/CleanPlanView',
    'mvc/ProgressbarView',
    'mvc/SourceEditorView',
    'text!mvc/template/controller.template.html',
    'text!mvc/template/detail.template.html'
], function(
    Requester,
    Table,
    FileDrop,
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
        ProgressBarView.start('progressbar');
        domId = id;
        renderSourceList();
    }

    function getSelectedPlan() {
        return $("#selected_rule").val();
    }

    function getSelectedSource() {
        return $("#selected_source").val();
    }

    function renderSourceList() {
        Requester.getSource(
            function(source) {
                var sources = source['data'];
                Requester.getRule(
                    function(data) {
                        var plans = data['data'];
                        var html =
                            _.template(ControllerTemplate)
                                ({ sources: sources, plans: plans});
                        $('#' + domId).html(html);

                        // render the source modal
                        SourceEditorView.start('source-editor-modal');
                        bindEvent();
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
                function(data) {
                    info("Selected rules are deleted.");
                    // force a refresh
                    renderSourceList();
                }
            );
        });
    }

    function renderRuleDetail() {
        var selectedPlan = getSelectedPlan();
        if (_.isUndefined(selectedPlan) || _.isNull(selectedPlan)) {
            return;
        }

        if (_.isArray(selectedPlan) && selectedPlan.length != 1) {
            $('#detail').html('');
        } else {
            Requester.getRuleDetail(
                selectedPlan,
                function (data) {
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
        var activeTable = $('#tables').find('li.active')[0].id;
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
                        info("A repair job is successfully submitted.");
                        var key = data['data'];
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
        Requester.deleteViolation(
            function() {
                _.each(plans, function(planName) {
                    Requester.getRuleDetail(
                        planName,
                        function(data) {
                            var plan = arrayToPlan(data['data'][0]);
                            Requester.doDetect(
                                plan,
                                function(data, status) {
                                    info("A job is successfully submitted.");
                                    var key = data['data'];
                                    console.log('Received job key : ' + key);
                                    if (key != null) {
                                        window.localStorage[key] = plan.name;
                                    }
                                },

                                function(data, status) {
                                    err("<strong>Error</strong>: " + data.responseText);
                                }
                            )
                        }
                     );
                });
            }
        );
    }

    function bindEvent() {
        $('#refresh_rule').on('click', function(e) {
            renderSourceList();
        });

        $('#refresh_source').on('click', function(e) {
            renderSourceList();
        });

        $('#new_plan').on('click', function(e) {
            CleanPlanView.render(
                'cleanPlanView',
                {name: null, type: 'FD', source: null, tablename: null}
            );
        });

        $('#cleanPlanPopup').on('hidden', function(e) {
            renderSourceList();
            // info('You just successfully updated a rule.');
        });

        $('#edit_plan').on('click', function(e) {
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

        $('#new-source').on('click', function(e) {
            $('#source-editor').modal('show');
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

        $('#delete').on('click', function(e) {
            var selectedPlan = getSelectedPlan();
            if (selectedPlan == null || !_.isArray(selectedPlan)) {
                err('No rule is selected.');
                return;
            }

            deleteRule(selectedPlan);
        });

        $('#repair').on('click', function(e) {
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
