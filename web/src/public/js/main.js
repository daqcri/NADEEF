require.config({
    baseUri: '.',
	paths: {
		"text" : "lib/text",
		"jquery" : "lib/jquery-1.7.1.min",
		"underscore" : "lib/underscore-min",
		"bootstrap" : "lib/bootstrap.min",
		"datatable" : "lib/jquery.dataTables",
		"d3" : "lib/d3.v3",
		"nvd3" : "lib/nv.d3",
		"ace" : "lib/ace-min/ace",
	},

	shim: {
        'jquery' : {
            exports: '$',
        },

        'underscore' : {
            deps : ['jquery'],
            exports: '_',
        },
        
		'bootstrap' : {
			deps : ['jquery']
		},

        'd3' : {
            deps : ['jquery']
        },

		'nvd3' : {
			deps : ['d3']
		},

		'table' : {
			deps : ['jquery', 'bootstrap', 'datatable'],
			exports: 'Table'
		},

		'ace' : {
			deps : ['jquery'],
			exports: 'ace'
		}
	}
});

// render the first page
require(
    ['boilerplate', 'router', 'mvc/NavbarView'],
    function(Bootstrap, Router, NavbarView) {
        Bootstrap.start();
        Router.start();
        NavbarView.start();
        $('#home').trigger('click');

        // DashboardView.start();
        // initial click on the home page
    }
);
