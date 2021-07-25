# Created by Remzi at 01/02/2021
Feature: stag-csv-to-raw-parquet
  # Enter feature description here

  @test2
  @NBA-stag
  Scenario: NBA
    Given Lets Start
    When I upload "days" to "rt-stag"

