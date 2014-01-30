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
    "text!template/home.template.html"
], function(
    HomeTemplate
) {
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
