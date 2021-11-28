package aueb.bigdata.project

import com.googlecode.charts4j.{Color, GCharts, Slice}

import scala.collection.JavaConverters

object Chart {

  case class Draw(id: Int, name: String, age: Int, friends: Long)

  def drawPieChart(drawArgs: scala.collection.Map[String, Int]): String = {
    drawArgs.foreach(e => println((e._2, e._1)))
    val slices = drawArgs.map(e => Slice.newSlice(e._2, e._1)).toArray
    val pieChart = GCharts.newPieChart(JavaConverters.mutableSeqAsJavaList(slices))
    pieChart.setTitle("Crime genre ratings pie chart", Color.BLACK, 15)
    pieChart.setSize(720, 360)
    pieChart.setThreeD(true)
    pieChart.toURLString;
  }
}
