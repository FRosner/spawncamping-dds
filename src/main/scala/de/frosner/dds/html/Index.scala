package de.frosner.dds.html

import de.frosner.dds.servables.c3.Chart

object Index {

  val html =
    <html>
      <head>
        <link href="/css/c3.css" rel="stylesheet" type="text/css"></link>
        <link href="/css/table.css" rel="stylesheet" type="text/css"></link>
        <script src="/lib/d3.js" charset="utf-8"></script>
        <script src="/lib/c3.js"></script>
        <script src="/lib/jquery.js"></script>
      </head>
      <body>
        <div id={Chart.id}></div>
        <script src="/app/main.js"></script>
      </body>
    </html>

}
