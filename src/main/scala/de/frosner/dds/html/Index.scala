package de.frosner.dds.html

import de.frosner.dds.util.StringResource

object Index {

  lazy val html =
    <html>
      <head>
        <link href="/css/c3.css" rel="stylesheet" type="text/css"></link>
        <link href="/css/table.css" rel="stylesheet" type="text/css"></link>
        <link href="/css/index.css" rel="stylesheet" type="text/css"></link>
        <link href="/css/d3.parcoords.css" rel="stylesheet" type="text/css"></link>
        <script src="/lib/d3.js" charset="utf-8"></script>
        <script src="/lib/d3.parcoords.js" charset="utf-8"></script>
        <script src="/lib/c3.js"></script>
        <script src="/lib/jquery.js"></script>
      </head>
      <body>
        <div id="header">
          <div id={LockButton.id} onclick="toggleUpdating()" class="unlocked" title="Lock Vizboard"></div>
        </div>
        <div id="content">
          <object data="/img/watermark.svg" type="image/svg+xml" id={Watermark.id} style="display: block; width: 50%; margin: 0 auto;"></object>
          <script src="/app/main.js"></script>
        </div>
      </body>
    </html>

  lazy val css = StringResource.read("/css/index.css")

}
