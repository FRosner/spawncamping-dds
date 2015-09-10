package de.frosner.dds.servables.c3

import de.frosner.dds.core.Servable

abstract class CategoricalChart(data: Data, categories: Seq[String], title: String)
  extends Chart(data, XAxis.categorical(categories), title)
