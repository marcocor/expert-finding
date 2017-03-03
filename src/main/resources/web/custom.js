$('#search-button').click(issueQuery);

$('#query-input').keypress(function (event) {
	if (event.which == 13) {
		event.preventDefault();
    	issueQuery();
  	}
});

$("#author-search").select2({
  ajax: {
    url: "/completion",
    dataType: 'json',
    delay: 150,
    data: function (params) {
      return {
        q: params.term,
      };
    },
    processResults: function (data) {
      return {
        results: $.map(data.authors, function (author) {
				          return {
				            text: author.name,
				            id: author.id,
				            institution: author.institution,
				          }
                     })
            };
    },
    cache: true
  },
  escapeMarkup: function (markup) { return markup; },
  templateResult: function (author) {return author.text + " (" + author.institution + ") id=" + author.id},
  minimumInputLength: 2,
})
.change(
		function () {
			author_id = $(this).val();
			updateAndShowAuthorModal(author_id);
		}
)


function issueQuery() {
	query = $('#query-input').val();
	$('#loader').show();
	
    var queryAPI = "/query";
    $.getJSON(queryAPI, {"q": query,})
	.done(
		function(data) {
			$('#results').show();
			fillResults($("#results-list-cos-ef-iaf"), data["experts_cossim_efiaf"], data["query_entities"]);
			fillResults($("#results-list-ef-iaf"), data["experts_efiaf"], data["query_entities"]);
			fillResults($("#results-list-ec-iaf"), data["experts_eciaf"], data["query_entities"]);
			fillResults($("#results-list-log-ec-ef-iaf"), data["experts_log_ec_ef_iaf"], data["query_entities"]);
			$("#time-cos-ef-iaf").text(data["time_cossim_efiaf"].toFixed(3) + " sec")
			$("#time-ef-iaf").text(data["time_efiaf"].toFixed(3) + " sec")
			$("#time-ec-iaf").text(data["time_eciaf"].toFixed(3) + " sec")
			$("#time-log-ec-ef-iaf").text(data["time_log_ec_ef_iaf"].toFixed(3) + " sec")
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

function updateAndShowDocumentModal(author_id, author_name, query_entities){
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
				tbody = $("<tbody>")
				$.each(docdata["entities"], function(i, e) {
					tbody.append(
						$("<tr>")
							.append($("<td>").text(e["entity"]))
							.append($("<td>").text(e["count"]))
					)
				} 
				)

				table = $("<table>").attr("class", "table")
						.append($( "<thead><tr><th>Entity</th><th>Occ.</th></tr></thead>" ))
						.append(tbody)


				popover_body = $("<div>").append($("<span>").text("Year "+docdata["year"])).append(table)
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

function updateAndShowAuthorModal(author_id){
	$("#author-modal-doc-list").empty()
    var queryAPI = "/author";
    $.getJSON(queryAPI, {
    						"id": author_id,
    					}
    )
	.done(
		function(data) {
			$(".author-modal-author-name").text(data.name)
			$("#author-modal-author-id").text(data.id)
			$("#author-modal-author-doc-count").text(data.papers_count)
		}
	)
	.fail(
		function(data) {
			alert("Author request failed.")
		}
	)

	$("#author-modal").modal()
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
						updateAndShowDocumentModal(r["author_id"], r["name"], query_entities)
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