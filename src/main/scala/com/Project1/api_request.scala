package com.Project1

object api_request extends App{

  val url = "https://api-football-v1.p.rapidapi.com/v3/teams/statistics"
  val leagues = "https://api-football-v1.p.rapidapi.com/v3/teams"
  val transfers = "https://api-football-v1.p.rapidapi.com/v3/transfers"
  val players = "https://api-football-v1.p.rapidapi.com/v3/players"
  val squads = "https://api-football-v1.p.rapidapi.com/v3/players/squads"
  val standings = "https://api-football-v1.p.rapidapi.com/v3/standings"
  val key1 = "x-rapidapi-host"
  val value1 = "api-football-v1.p.rapidapi.com"
  val key2 = "x-rapidapi-key"
  val value2 = "de9cd023c8msh4ee597d6ef41ccfp19de46jsn9a9f6c07be91"
  val headers:Map[String, String] = Map(key1->value1, key2->value2)

  val league_payload:Map[String, String] = Map("league"-> "39", /*"team" -> "40",*/ "season" -> "2020")
  val players_payload:Map[String, String] = Map("id" -> "931","league"-> "39", "season" -> "2021")
  val transfers_payload:Map[String, String]= Map("team"-> "33")
  val squad_payload: Map[String,String] = Map("team" -> "50")
  val standings_payload: Map[String, String] = Map("season" -> "2021", "league" -> "39")
  val responses = collection.mutable.Buffer.empty[ujson.Value]

  val r = requests.get(
    players,
    //params = league_payload,
    //params = transfers_payload,
    params = players_payload,
    //params = squad_payload,
    //params = standings_payload,
    headers = headers

  )

  val parsed = ujson.read(r)

  println(parsed.render(indent = 4))
}
