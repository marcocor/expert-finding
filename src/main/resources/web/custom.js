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
			fillResults($("#results-list-right"), data["experts_efiaf"], data["query_entities"]);
			fillResults($("#results-list-left"), data["experts_cossim_efiaf"], data["query_entities"]);
			$("#time-right").text(data["time_efiaf"].toFixed(3) + " sec")
			$("#time-left").text(data["time_cossim_efiaf"].toFixed(3) + " sec")
			fillQueryEntities($("#query-entities"), data["query_entities"])
		}
	)
	.fail(
		function(data) {
			alert("Query failed.")
		}
	)
	.always(
		function(data) {
			$('#loader').hide();
		}
	)
}

function activeEntitiesModal() {
	return $(".entity-button").filter(".active").map(
		function () {
			return $(this).attr("entity")
		}
	).get()
}

function refreshModalAnnotations(){
	if ($(this).hasClass("active")) {
		$(this).removeClass("active")
		$(this).attr("aria-pressed", "false")
	} else {
		$(this).addClass("active")
		$(this).attr("aria-pressed", "true")
	}
	$(".annotation").removeClass("highlight-annotation")
	$.each(activeEntitiesModal(), function(i, e){
		$(".annotation").filter("[entity='"+e+"']").addClass("highlight-annotation")
	})
}

function refreshModalDocument(){
    var queryAPI = "/document";
    $.getJSON(queryAPI, {"d": $(this).attr("doc-id"),})
	.done(
		function(data) {
			$("#annotations-modal-doc-body").html(data['body'])
			refreshModalAnnotations()
		}
	)
	.fail(
		function(data) {
			alert("Document request failed.")
		}
	)
}

function updateAndShowModal(author_id, author_name, query_entities){
	$("#annotations-modal-author-name").text(author_name + " (id " + author_id + ")")
	$("#annotations-modal-doc-body").empty()
	$("#annotations-modal-doc-list").empty()
    var queryAPI = "/documents";
    $.getJSON(queryAPI, {
    						"a": author_id,
    						"e": JSON.stringify(query_entities)
    					}
    )
	.done(
		function(data) {
			$.each(data, function(docid, docdata) {
				tbody = $("<tbody>").append($( "<thead><tr><th>Entity</th><th>Occ.</th></tr></thead>" ))
				$.each(docdata["entities"], function(i, e) {
					tbody.append(
						$("<tr>")
							.append($("<td>").text(e["entity"]))
							.append($("<td>").text(e["count"]))
					)
				} 
				)
				popover_body = $("<div>").append($("<span>").text("Year "+docdata["year"])).append($("<table>").attr("class", "table").append(tbody))
				$("#annotations-modal-doc-list").append(
					$("<li>").addClass("list-group-item")
						.attr("data-container", "body")
						.attr("data-toggle", "popover")
						.attr("data-trigger", "hover")
						.attr("doc-id", docid)
						.text(docid).click(refreshModalDocument)
						.popover({trigger: "hover", container:"body", placement: "left", html: true, content: popover_body})  
					)
				}
			)
		}
	)
	.fail(
		function(data) {
			alert("Author documents request failed.")
		}
	)

	$("#annotations-modal").modal()
}

function fillResults(li, results, query_entities) {
	li.empty();
	$.each(results, function(i, r) {
		li.append($('<li>')
				.addClass("list-group-item")
				.attr("author-id", r["author_id"])
				.attr("author-name", r["name"])
				.text(r["name"])
				.hover(
					function() {
    					$("[author-id='"+ r["author_id"] +"']").addClass("list-group-item-success");
    					},
    				function() {
    					$("[author-id='"+ r["author_id"] +"']").removeClass("list-group-item-success");
    					}
				)
				.click(
					function() {
						updateAndShowModal(r["author_id"], r["name"], query_entities)
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

	$("#annotations-modal-entity-buttons").empty()
	$.each(query_entities, function(i,e) {
		$("#annotations-modal-entity-buttons").append(
				$("<button>").attr("type", "button").attr("entity", e)
				.addClass("active")
				.attr("aria-pressed", "true")
				.addClass("btn").addClass("btn-default").addClass("entity-button")
				.text(e).click(refreshModalAnnotations)
			)
		}
	)


}