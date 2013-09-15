define([
    "render",
    "text!mvc/template/tab.template.html",
    "text!mvc/template/dashboard.template.html"
], function(renderer, widget_template, dashboard_template) {
    function start() {
        render();

        renderer.drawOverview('overview');
        renderer.drawAttribute('attribute');
        renderer.drawDistribution('distribution');
		renderer.drawTupleRank('tupleRank');
    }
    
    function render() {
        var tabTemplate = _.template(widget_template);
        var overviewHtml =
            tabTemplate({tabs:
                [{tag : "overview", head : "Overview", isActive : true}]
            });
        var attributeHtml =
            tabTemplate({tabs:
                [{tag : "attribute", head : "Rule Attribute", isActive : true}]
            });
        var distributionHtml =
            tabTemplate({tabs:
                [{tag : "distribution", 
                  head : "Rule Distribution", 
                  isActive : true}]
            });
        var tupleRankHtml =
            tabTemplate({tabs:
                [{tag : "tupleRank",
                  head : "Tuple Rank",
                  isActive : true}]
            });
        
        var dashboardTemplate = _.template(dashboard_template);
        var dashboardHtml = dashboardTemplate(
            {
                placeholder1: overviewHtml,
                placeholder2: attributeHtml,
                placeholder4: distributionHtml,
                placeholder3: tupleRankHtml
            });

        $('#container').html(dashboardHtml);
    }
    
    return {
        start: start
    };
});
