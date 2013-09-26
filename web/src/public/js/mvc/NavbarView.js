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
    "text!mvc/template/navbar.template.html"
], function(Router, NavbarTemplate) {
    function bindEvent() {
        $('#navbar').find('li a').on('click', function() {
            $('#navbar').find('.active').removeClass('active');
            $(this).parent().addClass('active');
        });

        $("#project-button").on("click", function() {
            var newProject = $("#create-new-project").val();
            var selectedProject = $("#select-existing-project").val();

            // TODO: project validation here
            var state;
            if (newProject != null && newProject != "") {
                state = { "name" : newProject, "create" : true };
            } else {
                state = { "name" : selectedProject, "create" : false };
            }

            $("#projectModal").modal('hide');
            Router.redirect('#home', state);
        });
    }
    
    function render() {
        $('body').append(_.template(NavbarTemplate)());
    }
    
	function start() {
		render();
		bindEvent();

        $("#projectModal").modal('show');
	}
	
    return {
        start : start
    };
});
