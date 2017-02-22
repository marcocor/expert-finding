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