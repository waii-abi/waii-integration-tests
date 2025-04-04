# Details

  - tweakit_doc.pdf - Has the following schema details (main tables, additional tables, duplicate/conflicting tables)
  - tweakit_db_owner_storage.xlsx - Has only one table definition in xlsx format
  - tweakit_contradictory_definitions.xlsx - Has all the tables with contradictory definitions




# Database Schema Overview

This document provides a high-level overview of the schema for the `tweakit` database. It outlines the original tables, additional tables introduced for extended functionality, and duplicate table definitions designed to test conflict resolution and data consistency.

---

## Original Schema Tables

These tables form the core schema used in production:

- db_connections
- oauth_configurations
- access_tokens
- wabe_configs
- access_keys
- llm_usage_storage
- users
- organizations
- tenants
- db_alias
- db_owner_storage
- parameters
- query_gen_history
- shared_connection_search_contexts

---

## Additional Tables

These tables have been introduced to extend functionality:

- **system_logs**: Captures system-level events and errors with log levels, timestamps, and detailed messages.
- **user_preferences**: Stores personalized settings for users, including preference keys, values, and update timestamps.
- **api_rate_limits**: Monitors and enforces API rate limits with fields for API keys, endpoints, limits, and tracking information.

---

## Duplicate/Conflicting Table Definitions

For testing and reference, duplicate versions of some tables have been created. These duplicates intentionally include conflicting details:

- **users_temp**:
  - **Conflicts**: The primary key `id` is defined as an integer (vs. text in the original). It includes extra columns such as `email` and `created_at` which are not present in the original `users` table.
   
- **llm_usage_storage_duplicate**:
  - **Conflicts**: Adds a `usage_notes` column for extra metadata. This column is absent in the original `llm_usage_storage` table.

---

## Summary
- Also, some tables will have descriptions in starting, some at the end etc. All these are intensional.

