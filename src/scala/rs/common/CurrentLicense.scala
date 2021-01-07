package rs.common


object CurrentLicense {
  var license = ""
  var licenseCode = ""

  def init(license: String): Unit = {
    this.license = license
    this.licenseCode = license match {
      case "wasu" => "1015"
      case "tencent" => "1011"
      case "youku" => "1007"
      case "mobile" => "1000"
      case "child" => "child"
      case "edu" => "edu"
      case "shop" => "shop"
      case "app" => "app"
      case "game" => "game"
    }
  }

}
