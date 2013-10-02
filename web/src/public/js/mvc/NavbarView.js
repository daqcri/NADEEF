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
    "requester",
    "text!mvc/template/navbar.template.html",
    "text!mvc/template/project.template.html",
    "mvc/HomeView"
], function(Router, Requester, NavbarTemplate, ProjectTemplate, HomeView) {
    function bindEvent() {
        $('#navbar').find("li a").on('click', function() {
            $('#navbar').find('.active').removeClass('active');
            $(this).parent().addClass('active');
        });

        $("#refresh").on('click', HomeView.refresh);

        $("#change").on("click", function() {
            $('#projectModal').find('.modal-body').remove();
            startProject('projectModal');
        });

        $("#project-button").on("click", function() {
            var newProject = $("#create-new-project").val();

            // TODO: project validation here
            if (newProject != null && newProject != "") {
                Requester.createProject(newProject, function() {
                    $("#projectModal").modal('hide');
                    Router.redirect('#home', { name : newProject });
                });
            } else {
                var selectedProject = $("#select-existing-project").val();
                $("#projectModal").modal('hide');
                Router.redirect('#home', { name : selectedProject });
            }
        });
    }
    
    function render() {
        $('body').append(_.template(NavbarTemplate)());
    }

    function startProject(id) {
        Requester.getProject(function(data) {
            var projects = data['data'];
            var modalHtml = _.template(ProjectTemplate) (
                {
                    projects: projects
                }
            );
            $('#' + id).find(".modal-footer").before(modalHtml);
            $('#' + id).modal('show');
        });
    }

	function start() {
		render();
		bindEvent();

        startProject('projectModal');
	}
	
    return {
        start : start
    };
});
