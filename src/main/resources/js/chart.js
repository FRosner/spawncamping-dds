

$(document).ready(function(){
    setInterval("checkForUpdate()",100);
});

function requestChart() {
    var chart = c3.generate({
        bindto: '#chart',
        data: {
          columns: [
            ['data1', 30, 200, 100, 400, 150, 250],
            ['data2', 50, 20, 10, 40, 15, 25]
          ]
        }
    });
}

function checkForUpdate() {
    $.ajax({
        url: "http://localhost:8080/chart/update",
        success: function(result) {
            if (result > 0) {
                requestChart();
            }
        }
    });
}

