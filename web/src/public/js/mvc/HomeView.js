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
    "router",
    "render",
    "table",
    "requester",
    "text!mvc/template/home.template.html",
    "text!mvc/template/table.template.html",
    "text!mvc/template/tab.template.html",
    "mvc/ControllerView"
], function(
    Router,
    Renderer,
    Table,
    Requester,
    HomeTemplate,
    TableTemplate,
    WidgetTemplate,
    ControllerView
) {
    function info(msg) {
        $('home-alert-info').alert('close');
        $('#home-alert').html([
            ['<div class="alert alert-success" id="home-alert-info">'],
            ['<button type="button" class="close" data-dismiss="alert">'],
            ['&times;</button>'],
            ["<span><h4>" + msg + "</h4></span></div>"]].join(''));
    
        window.setTimeout(function() { $('#home-alert-info').alert('close'); }, 2000);
    }
    
    function err(msg) {
        $('#home-alert').html([
            ['<div class="alert alert-error">'],
            ['<button type="button" class="close" data-dismiss="alert">'],
            ['&times;</button>'],
            ['<span>' + msg + '</span></div>']].join(''));  
    }

    function bindEvent() {
        $('#widget').find("ul li").on('click', function(e) {
            renderWidget(e.currentTarget.id);
        });

        $('#table-tabs a[data-toggle="tab"]').on('click', function (e) {
            console.log('second');
        })

        $('#clear').on('click', function() {
            Requester.deleteViolation(
                function() {
                    info('Violations are removed');
                    refresh();
                }
            );
        });
    }

    function refresh() {
        var defaultWidget = $('#widget').find('li.active');
        if (defaultWidget != null) {
            renderWidget(defaultWidget[0].id);
        }

        var defaultTable = $('#tables').find('li.active');
        if (defaultTable != null) {
            renderTable(defaultTable[0].id);
        }           
    }
    
    function start() {
        render();
        bindEvent();
        // render default view
        var defaultWidget = $('#widget').find('li.active');
        if (defaultWidget != null && defaultWidget.length > 0) {
            renderWidget(defaultWidget[0].id);
        } else {
            renderWidget("overview");
        }

        var defaultTable = $('#tables').find('li.active');
        if (defaultTable != null && defaultTable.length > 0) {
            renderTable(defaultTable[0].id);
        } else {
            renderTable("violation");
        }
    }

    function renderTable(id) {
        console.log('Load table ' + id);
        switch(id) {
        case 'tab_violation':
            Table.load('violation');
            break;
        case 'tab_audit':
            Table.load('audit');
            break;
        case 'tab_source':
            var sourceTable = $("#selected_source").val();
            if (!_.isEmpty(sourceTable)) {
                Table.load(sourceTable);
            }
            break;
        }
    }
    
    function renderWidget(id) {
        console.log('Render ' + id);
        switch (id) {
        case 'tab_overview':
            Renderer.drawOverview('overview');
            break;
        case 'tab_attribute':
            Renderer.drawAttribute('attribute');
            break;
        case 'tab_distribution':
            Renderer.drawDistribution('distribution');
            break;
        case 'tab_tupleRank':
            Renderer.drawTupleRank('tupleRank');
            break;
        case 'tab_violationRelation':
            Renderer.drawViolationRelation('violationRelation');
            break;
        }
    }
    
    function render() {
        var widgetTabs = {
            tabs: [
                {tag : "overview", head : "Overview", isActive : true},
                {tag : "attribute", head : "Rule Attribute", isActive : false},
                {tag : "distribution", head : "Rule Distribution", isActive : false},
                {tag : "violationRelation",
                 head : "Violation Relation",
                 isActive : false},
                {tag : "tupleRank", head : "Tuple Rank", isActive : false}
            ]
        };

        var homeHtml = _.template(
            HomeTemplate, {
                placeholder1 : _.template(WidgetTemplate)(widgetTabs),
                placeholder2 : _.template(TableTemplate)()
            });

        $("#container").html(homeHtml);         
        
        // render the cleanplan controller
        ControllerView.render('controller');        
    }
    
    return {
        start: start,
        refresh: refresh
    };
});
