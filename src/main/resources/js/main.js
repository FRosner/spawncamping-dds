$(document).ready(function(){
    setInterval("checkForUpdate()",100);
});

function checkForUpdate() {
    $.ajax({
        url: "http://localhost:8080/chart/update",
        success: function(response) {
            if (response != "{}") {
                var chart = c3.generate(JSON.parse(response));
            }
        }
    });
}

