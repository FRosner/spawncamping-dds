package de.frosner.dds.html

import de.frosner.dds.util.StringResource

object Index {

  lazy val html =
    <html>
      <head>
        <link href="/ui/css/c3.css" rel="stylesheet" type="text/css"></link>
        <link href="/ui/css/table.css" rel="stylesheet" type="text/css"></link>
        <link href="/ui/css/index.css" rel="stylesheet" type="text/css"></link>
        <link href="/ui/css/d3.parcoords.css" rel="stylesheet" type="text/css"></link>
        <script src="/ui/lib/d3.v3.min.js" charset="utf-8"></script>
        <script src="/ui/lib/d3.parcoords.js" charset="utf-8"></script>
        <script src="/ui/lib/c3.min.js"></script>
        <script src="/ui/lib/jquery-1.11.2.min.js"></script>
      </head>
      <body>
        <div id="header">
          <div id={LockButton.id} onclick="toggleUpdating()" class="unlocked" title="Lock Vizboard"></div>
        </div>
        <div id="content">
          <object data="/ui/img/watermark.svg" type="image/svg+xml" id={Watermark.id} style="display: block; width: 50%; margin: 0 auto;"></object>
          <script src="/ui/app/main.js"></script>
        </div>
      </body>
    </html>

  lazy val css = StringResource.read("/ui/css/index.css")

}
