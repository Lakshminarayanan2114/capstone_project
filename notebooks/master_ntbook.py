# Databricks notebook source
dbutils.notebook.run(
    "./bronze_analysis",
    timeout_seconds=0
)

dbutils.notebook.run(
    "./silver_analysis",
    timeout_seconds=0
)

dbutils.notebook.run(
    "./gold_analysis",
    timeout_seconds=0
)