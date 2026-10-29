# KrishiFlow — Farm-to-Market Data Intelligence Platform

KrishiFlow is a data engineering and business intelligence project that combines agricultural supply-chain operations with external weather and mandi-price data.

The goal is to analyze crop production, collection, storage, transportation, sales, weather conditions, and market-price patterns using a medallion architecture.

## Technology Stack

- Python and PySpark
- Databricks Free Edition
- Delta Lake and SQL
- Bronze, Silver, and Gold architecture
- Data-quality validation and quarantine
- Delta MERGE for Gold-layer upserts
- Databricks Jobs for orchestration
- Power BI for interactive reporting (in progress)

## Architecture

Source data
→ Bronze Delta tables
→ Silver cleaning and validation
→ Gold dimensions, fact tables, and analytics views
→ Power BI dashboard

## Data Sources

- **Operational data:** Synthetic farmer, farm, crop, harvest, collection, storage, transport, and sales records.
- **Weather:** Historical daily weather observations from Open-Meteo.
- **Mandi prices:** Government of India data.gov.in dataset downloaded as a CSV because direct API access was unavailable in the development environment.

## Current Implementation

- Bronze tables for source datasets
- Silver transformations, type conversion, and validation
- Duplicate-key and foreign-key checks
- Quarantine handling for invalid records
- Gold dimension and fact tables
- Business-facing analytics views
- Incremental upserts for Gold tables
- Databricks Job orchestration configured
- Power BI semantic model and dashboard under development

## Data Limitations

Operational supply-chain records are simulated and are not verified real-world farm records.

The mandi-price data is a downloaded snapshot, not a complete year of historical prices. Government mandi names are kept separate from synthetic operational market references unless a mapping can be verified.

Transport distances in the synthetic operational data are estimates.

## Repository Structure

- `databricks/notebooks/` — Databricks notebook source files
- `architecture/` — Architecture diagrams
- `docs/` — Data dictionary and project documentation
- `powerbi/` — Power BI project files and report screenshots

## Purpose

This project demonstrates practical data engineering, data-quality management, dimensional modeling, incremental processing, orchestration, and business intelligence skills.
