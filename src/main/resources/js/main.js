$(document).ready(function(){
    setInterval("checkForUpdate()",100);
});

function checkForUpdate() {
    $.ajax({
        url: "http://localhost:8080/chart/update",
        success: function(response) {
            if (response != "{}") {
                var servable = JSON.parse(response);
                if (servable.type == "chart") {
                    var chart = c3.generate(servable.content);
                } else if (servable.type == "stats") {
                    document.getElementById("chart").innerHTML = servable.content
                } else {
                    console.log("Unrecognized response: " + response);
                }
            }
        }
    });
}

