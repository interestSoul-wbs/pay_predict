package rs.common

import scala.xml.{NodeSeq, XML}

class Recommendation {
  private val fieldRecommendation = "Recommendation"
  private val fieldName = "Name"
  private val fieldArgument = "Argument"
  private val fieldValue = "Value"
  private val fieldLicense = "License"
  var argsMap: Map[String, String] = Map()
  var license: String = _

  def analyze(xmlPath: String, scenarioType: ScenarioType.Value): this.type = {
    val xmlFile = XML.load(xmlPath)
    val licenseNodes = xmlFile \ fieldLicense
    val licenseNode = licenseNodes.head
    license = licenseNode.text
    val recNodes = xmlFile \\ fieldRecommendation
    val recNodesFilter = recNodes.filter(recNode => {
      val name = (recNode \ fieldName).text.toString.toLowerCase()
      name == scenarioType.toString.toLowerCase()
    })
    if (recNodesFilter.length.>=(1)) {
      val recNode = recNodesFilter.head
      val argsNodes: NodeSeq = recNode \\ fieldArgument
      argsNodes.foreach(f = argsNode => {
        val name = (argsNode \ fieldName).text.toString
        val value = (argsNode \ fieldValue).text.toString.trim
        argsMap += (name -> value)
      })
    }
    this
  }

}
