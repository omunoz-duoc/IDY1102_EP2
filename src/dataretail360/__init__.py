"""DataRetail 360: pipeline de integración analítica.

PostgreSQL (14 tablas) + CSV + Excel -> Bronze -> Silver -> dimensiones SCD/snowflake
-> hechos -> DWH SQL Server -> 4 data marts dependientes construidos y probados con dbt.
"""
__version__ = '1.0.0'
