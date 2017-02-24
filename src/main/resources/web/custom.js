$('#search-button').click(issueQuery);

$('#query-input').keypress(function (event) {
	if (event.which == 13) {
		event.preventDefault();
    	issueQuery();
  	}
});

function issueQuery() {
	query = $('#query-input').val();
	$('#loader').show();
	
    var queryAPI = "/query";
    $.getJSON(queryAPI, {"q": query,})
	.done(
		function(data) {
			$('#results').show();
			fillResults($("#results-list-right"), data["experts_efiaf"]);
			fillResults($("#results-list-left"), data["experts_cossim_efiaf"]);
			$("#time-right").text(data["time_efiaf"].toFixed(3) + " sec")
			$("#time-left").text(data["time_cossim_efiaf"].toFixed(3) + " sec")
			fillQueryEntities($("#query-entities"), data["query_entities"])
		}
	)
	.fail(
		function(data) {
			alert("Request failed.")
		}
	)
	.always(
		function(data) {
			$('#loader').hide();
		}
	)
}

function fillResults(li, results) {
	li.empty();
	$.each(results, function(i, r) {
		li.append($('<li>')
				.addClass("list-group-item")
				.attr("author-id", r["author_id"])
				.text(r["name"])
				.hover(
					function() {
    					$("[author-id='"+ r["author_id"] +"']").addClass("list-group-item-success");
    					},
    				function() {
    					$("[author-id='"+ r["author_id"] +"']").removeClass("list-group-item-success");
    					}
				)
				.append($("<span>").addClass('badge').text(r["score"].toFixed(3))));
	});
}

function fillQueryEntities(div, query_entities){
	div.empty();

	if (query_entities.length == 0)
		div.append($('<div>')
				.addClass("alert").addClass("alert-warning").attr("role", "alert").text("No entities found in query."))
		
	$.each(query_entities, function(i, e) {
			div.append($('<span>').addClass("label").addClass("label-default").text(e));
		} 
	);
}