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
    "text!mvc/template/home.template.html",
    "text!mvc/template/tableentry.template.html",
    "text!mvc/template/detailpanel.template.html"
], function(
    HomeTemplate,
    TableEntryTemplate,
    DetailPanelTemplate
) {
    var carCache;
    var topCar;
    var keyword = null;
    var limit = 12;
    function info(msg) {
        $('#home-alert').html([
            ['<div class="alert alert-success" id="home-alert-info">'],
            ['<button type="button" class="close" data-dismiss="alert">'],
            ['&times;</button>'],
            ["<span>" + msg + "</span></div>"]].join(''));

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
        $("#detail-panel-close-button").on("click", function() {
            $("#detail-panel").css("display", "none");
        });

        $("#newcar-alert").on("click", function() {
            updateCar();
        });

        $("#more").on("click", function() {
            limit += limit;
            updateCar();
        });

        $("#search").on("change", function() {
            var val = $("#search").val();
            if (val.length < 4 || _.isNull(val))
                keyword = null;
            else
                keyword = val;

            updateCar();
        });

        setInterval(function() {
            if (_.isNull(carCache) || !_.isNumber(topCar)) {
                updateCar();
            } else {
                $.ajax({
                    url: "/delta?id=" + topCar,
                    success: function (x, e) {
                        if (!_.isNull(x) && _.isArray(x) && !_.isEmpty(x)) {
                            $("#newcar-alert span").empty();
                            var alertBar = d3
                                .selectAll("#newcar-alert span")
                                .selectAll("h4")
                                .data(x.filter(function (d) {
                                    return d.id > 0;
                                }), function(d) { return d.id});

                            alertBar
                                .enter()
                                .append("h4");

                            alertBar
                                .html(function (d) {
                                    return "<span class='badge badge-info'>" + d.id + "</span> new cars"
                                })
                                .style("opacity", 0)
                                .transition()
                                .duration(400)
                                .style("opacity", 1.0)
                        }
                    },
                    fail: function (x, e) {
                        console.log('query delta failed.');
                    }
                })
            }
        }, 3000);
    }

    function updateCar() {
        $.ajax({
            url:
                keyword == null ? "/top?limit=" + limit
                    : "/top?limit=" + limit + "&keyword=" + keyword,
            success: function(x, e) {
                if (_.isNull(x) || _.isEmpty(x)) {
                    return;
                }

                carCache = x;
                var dataMaxId = _.max(x, function(d) { return d.id }).id;
                if (!_.isUndefined(topCar) && _.isNumber(topCar))
                    topCar = _.max([dataMaxId, topCar]);
                else
                    topCar = dataMaxId;

                var rows =
                    d3.select("#carTableBody")
                    .selectAll("tr")
                    .data(x);

                rows.enter()
                    .append("tr");

                rows.html(function(d) {
                        return _.template(TableEntryTemplate, { entry: d});
                    })
                    .attr("id", function(d, i) {
                        return i;
                    })
                    .attr("class", function(d) {
                        if (_.isNull(d.brand_name) && _.isNull(d.model))
                            return "danger";
                        if (_.isNull(d.brand_name) || _.isNull(d.model))
                            return "warning";
                        return null;
                    })
                    .on('click', function(d) {
                        $("#carModal").html(
                            _.template(DetailPanelTemplate, { car : d })
                        );
                        $.ajax({
                            url: "/imgs?id=" + d.id,
                            success: function(r, e) {
                                if (_.isNull(r) || _.isEmpty(r)) {
                                    r = [{ url : "imgs/no_image.png" }];
                                }

                                var newImgs = d3.select("#imgs")
                                    .selectAll("div")
                                    .data(r, function(d) {
                                        return d.url;
                                    });

                                newImgs.enter()
                                    .append("div")
                                    .html(function(d) {
                                        return "<img src='" + d.url + "'>";
                                    })
                                    .attr("class", function(d, i) {
                                        if (i == 0) {
                                            return "item active"
                                        }
                                        return "item"
                                    });

                                newImgs
                                    .exit()
                                    .remove();
                            }
                        });

                        $.ajax({
                            url: "/dups?id=" + d.duplicate_group,
                            success: function(r, e) {
                                var contacts = d3.select("#contacts")
                                    .selectAll("a")
                                    .data(r);

                                contacts.enter()
                                    .append("a")
                                    .html(function(d, i) {
                                        return "Link " + (i + 1);
                                    })
                                    .attr("href", function(d, i) {
                                        return d.url;
                                    })
                                    .attr("target", function() {
                                        return "_blank";
                                    })
                                    .attr("class", function(d, i) {
                                        return "list-group-item list-group-item-info"
                                    });

                                contacts
                                    .exit()
                                    .remove();
                            }
                        });
                        $("#carModal").modal('show');
                    })
                    .style("opacity", 0)
                    .transition()
                    .duration(200)
                    .style("opacity", 1);

                rows.exit()
                    .style("opacity", 1)
                    .transition()
                    .duration(200)
                    .style("opacity", 0)
                    .remove();
                $("#newcar-alert span").empty();
            },
            fail: function(x, e) {
                console.log("failed.");
            }
        });
    }

    function start() {
        render();
        bindEvent();

    }

    function render() {
        var homeHtml = _.template(HomeTemplate);
        $("#container").html(homeHtml);
    }

    return {
        start: start
    };
});
