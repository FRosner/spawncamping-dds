package de.frosner.dds.html

object Index {

  val html =
    <html>
      <head>
        <link href="/css/c3.css" rel="stylesheet" type="text/css"></link>
        <link href="/css/table.css" rel="stylesheet" type="text/css"></link>
        <link href="/css/d3.parcoords.css" rel="stylesheet" type="text/css"></link>
        <!--    <script src="https://syntagmatic.github.io/parallel-coordinates/d3.parcoords.css" charset="utf-8"></script> -->
        <script src="/lib/d3.js" charset="utf-8"></script>
        <script src="/lib/d3.parcoords.js" charset="utf-8"></script>
        <!--    <script src="https://syntagmatic.github.io/parallel-coordinates/d3.parcoords.js" charset="utf-8"></script> -->
        <script src="/lib/c3.js"></script>
        <script src="/lib/jquery.js"></script>
      </head>
      <body>
        <object data="/img/watermark.svg" type="image/svg+xml" id={Watermark.id} style="display: block; width: 50%; margin: 0 auto;"></object>
        <script src="/app/main.js"></script>
      </body>
    </html>

}
