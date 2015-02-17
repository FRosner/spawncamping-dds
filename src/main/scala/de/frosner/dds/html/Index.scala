package de.frosner.dds.html

object Index {

  val html =
    <html>
      <head>
        <link href="/css/c3.css" rel="stylesheet" type="text/css"></link>
        <script src="/lib/d3.js" charset="utf-8"></script>
        <script src="/lib/c3.js"></script>
        <script src="/lib/jquery.js"></script>
      </head>
      <body>
        <div id="chart"></div>
        <script src="/app/chart.js"></script>
      </body>
    </html>

}
