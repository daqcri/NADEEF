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

define(["jquery", "bootstrap", "datatable"], function() {
	var instance = null;
	function initialize() {
		$.fn.dataTableExt.oApi.fnGetColumnData =
			function ( oSettings, iColumn, bUnique, bFiltered, bIgnoreEmpty ) {
				// check that we have a column id
				if ( typeof iColumn == "undefined" ) return new Array();				
				// by default we only want unique data
				if ( typeof bUnique == "undefined" ) bUnique = true;				
				// by default we do want to only look at filtered data
				if ( typeof bFiltered == "undefined" ) bFiltered = true;				
				// by default we do not want to include empty values
				if ( typeof bIgnoreEmpty == "undefined" ) bIgnoreEmpty = true;				
				// list of rows which we're going to loop through
				var aiRows;
				
				// use only filtered rows
				if (bFiltered == true) aiRows = oSettings.aiDisplay;
				// use all rows
				else aiRows = oSettings.aiDisplayMaster; // all row numbers
				
				// set up data array   
				var asResultData = new Array();				
				for (var i=0,c=aiRows.length; i<c; i++) {
					var iRow = aiRows[i];
					var aData = this.fnGetData(iRow);
					var sValue = aData[iColumn];
					
					// ignore empty values?
					if (bIgnoreEmpty == true && sValue.length == 0) continue;					
					// ignore unique values?
					else if (bUnique == true && $.inArray(sValue, asResultData) > -1) 
                        continue;
					
					// else push the value onto the result data array
					else asResultData.push(sValue);
				}
				
				return asResultData;
			};

		/* API method to get paging information */
		$.fn.dataTableExt.oApi.fnPagingInfo = function ( oSettings ) {
			return {
				"iStart":         oSettings._iDisplayStart,
				"iEnd":           oSettings.fnDisplayEnd(),
				"iLength":        oSettings._iDisplayLength,
				"iTotal":         oSettings.fnRecordsTotal(),
				"iFilteredTotal": oSettings.fnRecordsDisplay(),
				"iPage":          Math.ceil( oSettings._iDisplayStart / oSettings._iDisplayLength ),
				"iTotalPages":    Math.ceil( oSettings.fnRecordsDisplay() / oSettings._iDisplayLength )
			};
		};

		/*
		 * TableTools Bootstrap compatibility
		 * Required TableTools 2.1+
		 */
		if ( $.fn.DataTable.TableTools ) {
			// Set the classes that TableTools uses to something suitable for Bootstrap
			$.extend( true, $.fn.DataTable.TableTools.classes, {
				"container": "DTTT btn-group",
				"buttons": {
					"normal": "btn",
					"disabled": "disabled"
				},
				"collection": {
					"container": "DTTT_dropdown dropdown-menu",
					"buttons": {
						"normal": "",
						"disabled": "disabled"
					}
				},
				"print": {
					"info": "DTTT_print_info modal"
				},
				"select": {
					"row": "active"
				}
			} );

			// Have the collection use a bootstrap compatible dropdown
			$.extend( true, $.fn.DataTable.TableTools.DEFAULTS.oTags, {
				"collection": {
					"container": "ul",
					"button": "li",
					"liner": "a"
				}
			} );
		}

		/* Bootstrap style pagination control */
		$.extend( $.fn.dataTableExt.oPagination, {
			"bootstrap": {
				"fnInit": function( oSettings, nPaging, fnDraw ) {
					var oLang = oSettings.oLanguage.oPaginate;
					var fnClickHandler = function ( e ) {
						e.preventDefault();
						if ( oSettings.oApi._fnPageChange(oSettings, e.data.action) ) {
							fnDraw( oSettings );
						}
					};

					$(nPaging).addClass('pagination').append(
						'<ul>'+
							'<li class="prev disabled"><a href="#">&larr; '+oLang.sPrevious+'</a></li>'+
							'<li class="next disabled"><a href="#">'+oLang.sNext+' &rarr; </a></li>'+
							'</ul>'
					);
					var els = $('a', nPaging);
					$(els[0]).bind( 'click.DT', { action: "previous" }, fnClickHandler );
					$(els[1]).bind( 'click.DT', { action: "next" }, fnClickHandler );
				},

				"fnUpdate": function ( oSettings, fnDraw ) {
					var iListLength = 5;
					var oPaging = oSettings.oInstance.fnPagingInfo();
					var an = oSettings.aanFeatures.p;
					var i, j, sClass, iStart, iEnd, iHalf=Math.floor(iListLength/2);

					if ( oPaging.iTotalPages < iListLength) {
						iStart = 1;
						iEnd = oPaging.iTotalPages;
					}
					else if ( oPaging.iPage <= iHalf ) {
						iStart = 1;
						iEnd = iListLength;
					} else if ( oPaging.iPage >= (oPaging.iTotalPages-iHalf) ) {
						iStart = oPaging.iTotalPages - iListLength + 1;
						iEnd = oPaging.iTotalPages;
					} else {
						iStart = oPaging.iPage - iHalf + 1;
						iEnd = iStart + iListLength - 1;
					}

					for ( i=0, iLen=an.length ; i<iLen ; i++ ) {
						// Remove the middle elements
						$('li:gt(0)', an[i]).filter(':not(:last)').remove();

						// Add the new list items and their event handlers
						for ( j=iStart ; j<=iEnd ; j++ ) {
							sClass = (j==oPaging.iPage+1) ? 'class="active"' : '';
							$('<li '+sClass+'><a href="#">'+j+'</a></li>')
								.insertBefore( $('li:last', an[i])[0] )
								.bind('click', function (e) {
									e.preventDefault();
									oSettings._iDisplayStart = 
										(parseInt($('a', this).text(),10)-1) * oPaging.iLength;
									fnDraw( oSettings );
								} );
						}

						// Add / remove disabled classes from the static elements
						if ( oPaging.iPage === 0 ) {
							$('li:first', an[i]).addClass('disabled');
						} else {
							$('li:first', an[i]).removeClass('disabled');
						}

						if ( oPaging.iPage === oPaging.iTotalPages-1 || oPaging.iTotalPages === 0 ) {
							$('li:last', an[i]).addClass('disabled');
						} else {
							$('li:last', an[i]).removeClass('disabled');
						}
					}
				}
			}
		} );
	
	}

	function createSelect(data) {
		var r='<select><option value=""></option>', i, iLen=data.length;
		for ( i=0 ; i<iLen ; i++ ) {
			r += '<option value="'+data[i]+'">'+data[i]+'</option>';
		}
		return r+'</select>';
	}		

	function load(tablename) {
		if (instance != null) {
			instance.fnDestroy();
		}

		$.getJSON('/table/' + tablename, function(data) {
			var vdata = data['data'];
			var schema = data['schema'];
			var columns = new Array();
			$('#table').empty().append('<thead><tr></tr></thead>');
			for (var i = 0; i < schema.length; i ++) {
				columns.push( { sTitle : schema[i] } );
			}

			/* Add a select menu for each TH element in the table footer */
			instance = $('#table').dataTable({
				"sDom":
				"<'row-fluid'<'span6'l><'span6'f>r>t<'row-fluid'<'span6'i><'span6'p>>",
				"sPaginationType": "bootstrap",
				"bProcessing" : true,
				"bSort": true,
				"bDestroy": true,
				"aaData": vdata,
				"oLanguage": {
					"sLengthMenu": "_MENU_ entries per page"
				},
				"bAutoWidth" : false,
				"aoColumns": columns,
				"sWrapper" : "dataTables_wrapper form-inline"
			});

/*		
			var tfoot = '';
			for (var i = 0; i < schema.length; i ++) {
				tfoot += '<th>' + schema[i] + '</th>';
			}

			$('#table').append('<tfoot><tr>' + tfoot + '</tr></tfoot>');

			$("#tfoot th").each( function ( i ) {
				this.innerHTML = createSelect( instance.fnGetColumnData(i) );
				$('select', this).change( function () {
					instance.fnFilter( $(this).val(), i );
				});
			});
 */
		});
	}

    function filter(e) {
        if (instance != null)　{
            instance.fnFilter(e);
        }
    }
    
	return {
		load : load,
		init : initialize,
        filter : filter
	};
});

