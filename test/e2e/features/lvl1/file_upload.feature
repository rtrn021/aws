# Created by Remzi at 01/02/2021
Feature: stag-csv-to-raw-parquet
  # Enter feature description here

  @upload
  Scenario: upload
    Given Lets Start
    When I upload "iris" to "rt-stag"

