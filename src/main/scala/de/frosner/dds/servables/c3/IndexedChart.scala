package de.frosner.dds.servables.c3

import de.frosner.dds.core.Servable

abstract class IndexedChart(data: Data, title: String)
  extends Chart(data, XAxis.indexed, title)
