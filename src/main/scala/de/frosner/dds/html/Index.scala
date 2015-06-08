package de.frosner.dds.html

object Index {

  lazy val html =
    <html>
      <head>
        <link href="/ui/css/c3.css" rel="stylesheet" type="text/css"></link>
        <link href="/ui/css/table.css" rel="stylesheet" type="text/css"></link>
        <link href="/ui/css/index.css" rel="stylesheet" type="text/css"></link>
        <link href="/ui/css/graph.css" rel="stylesheet" type="text/css"></link>
        <link href="/ui/css/matrix.css" rel="stylesheet" type="text/css"></link>
        <link href="/ui/css/scatter.css" rel="stylesheet" type="text/css"></link>
        <link href="/ui/css/d3.parcoords.css" rel="stylesheet" type="text/css"></link>
        <link href="/ui/css/bootstrap.min.css" rel="stylesheet" type="text/css"></link>
        <script src="/ui/lib/d3.v3.min.js" charset="utf-8"></script>
        <script src="/ui/lib/d3.parcoords.min.js" charset="utf-8"></script>
        <script src="/ui/lib/c3.min.js"></script>
        <script src="/ui/lib/jquery-1.7.min.js"></script>
        <script src="/ui/lib/chroma.min.js"></script>
        <script src="/ui/lib/underscore.min.js"></script>
        <link rel="stylesheet" href="/ui/lib/slickgrid/slick.grid.css" type="text/css"/>
        <link rel="stylesheet" href="/ui/lib/slickgrid/jquery-ui-1.8.16.custom.css" type="text/css"/>
        <link rel="stylesheet" href="/ui/lib/slickgrid/slick.pager.css" type="text/css"/>
        <link rel="stylesheet" href="/ui/lib/slickgrid/examples.css" type="text/css"/>
        <script src="/ui/lib/jquery.event.drag-2.2.min.js"></script>
        <script src="/ui/lib/slickgrid/slick.core.min.js"></script>
        <script src="/ui/lib/slickgrid/slick.grid.min.js"></script>
        <script src="/ui/lib/slickgrid/slick.pager.min.js"></script>
        <script src="/ui/lib/slickgrid/slick.dataview.min.js"></script>
        <script src="/ui/lib/divgrid.min.js"></script>
      </head>
      <body>
        <div id="header" class="header">
          <input type="text" id="vizTitle" value="Visualization Title"/>
          <div id="lockButton" onclick="toggleUpdating()" class="headerButton unlocked" title="Lock Vizboard"></div>
        </div>
        <div id="content">
          <object data="/ui/img/watermark.svg" type="image/svg+xml" id="watermark" style="display: block; width: 50%; margin: 0 auto;"></object>
          <script src="/ui/app/util.js"></script>
          <script src="/ui/app/main.js"></script>
          <script src="/ui/app/Visualization.js"></script>
          <script src="/ui/app/Matrix.js"></script>
          <script src="/ui/app/Scatter2D.js"></script>
          <script src="/ui/app/Table.js"></script>
          <script src="/ui/app/C3Chart.js"></script>
          <script src="/ui/app/Histogram.js"></script>
          <script src="/ui/app/Graph.js"></script>
          <script src="/ui/app/Composite.js"></script>
        </div>
      </body>
    </html>

}
