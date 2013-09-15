define(["text!mvc/template/navbar.template.html"], function(page) {
    function bindEvent() {
        $('#navbar li a').on('click', function(e) {
            $('#navbar').find('.active').removeClass('active');
            $(this).parent().addClass('active');
        });
    }
    
    function render() {
        $('body').append(_.template(page)());
        bindEvent();
    }
    
	function start() {
		render();
		bindEvent();
	}
	
    return {
        start : start
    };
});
