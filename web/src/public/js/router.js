/* Simple routing system */
define([], function() {
    var routes =
        [{hash:'#home', controller:'HomeView'},
         {hash:'#dashboard',  controller:'DashboardView'}];
    var currentHash = '';
     
    function start() {
        window.location.hash = window.location.hash;
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
