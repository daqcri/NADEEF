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

/* Simple routing system */
define([], function() {
    var routes =
        [{hash:'#home', controller:'HomeView'},
         {hash:'#dashboard',  controller:'DashboardView'}];
    var currentHash = '';
     
    function start() {
        setInterval(hashCheck, 200);
    }
     
    function hashCheck() {
		if (window.location.hash == '') {
			window.location.hash = 'home';
		}
		
        if (window.location.hash != currentHash) {
            for (var i = 0, currentRoute; currentRoute = routes[i++];) {
                if (window.location.hash == currentRoute.hash) {
                    loadController(currentRoute.controller);
				}
            }
            currentHash = window.location.hash;
        }
    }
     
    function loadController(controllerName) {
        require(['mvc/' + controllerName], function(controller) {
            controller.start();
        });
    }
     
    return {
        start : start
    };
});
