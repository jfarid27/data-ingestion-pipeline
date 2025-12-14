# VideoCreatorStats

This package provides a comprehensive pipeline for ingesting, validating, processing, and analyzing video creator data. It merges creator profiles with their video content history to generate key performance metrics and engagement statistics.

## Overview

The `VideoCreatorStats` module performs the following operations:
1.  **Ingestion**: Loads raw CSV data for creators and videos.
2.  **Validation**: Applies configurable QA pipelines to ensure data integrity (types, null checks, constraints) using `config.py`.
3.  **Processing**:
    -   Joins video data with creator profiles.
    -   Calculates aggregate statistics such as average views and engagement rates.
    -   Computes a "viral score" based on engagement relative to follower count.
    -   Performs NLP analysis on video captions to extract top keywords using TF-IDF.
4.  **Storage**: Persists the processed results and raw datasets into Avro format with Hive-style partitioning for efficient downstream querying.

## Input Schemas

The pipeline expects two CSV files as input. The expected structure and validation rules are defined in `config.py`.

### `creators.csv`
| Column | Type | Description |
|--------|------|-------------|
| `creator_id` | Int64 | Unique identifier for the creator. Must be non-negative. |
| `username` | String | Creator's username. Cannot be empty. |
| `follower_count` | Int64 | Total number of followers. Must be non-negative. |
| `avg_views` | Int64 | Historical average views (from platform metadata). |
| `category` | String | Content category (e.g., Tech, Lifestyle). |
| `bio` | String | Creator's biography text. |

### `videos.csv`
| Column | Type | Description |
|--------|------|-------------|
| `video_id` | String | Unique identifier for the video. |
| `creator_id` | Int64 | Foreign key linking to `creators.csv`. |
| `views` | Int64 | Total view count. Must be non-negative. |
| `likes` | Int64 | Total like count. |
| `comments` | Int64 | Total comment count. |
| `shares` | Int64 | Total share count. |
| `caption` | String | Video caption text used for keyword extraction. |

## configuration (`config.py`)

The `config.py` file contains the Quality Assurance (QA) pipelines. Each pipeline is a list of dictionaries defining:
-   **`col`**: The column name to check.
-   **`type`**: The expected Pandas/Numpy data type.
-   **`fill_na`**: (Optional) Value to use for filling nulls.
-   **`assertions`**: A list of checks that run against the data. If `should_fail` is True, the pipeline raises an error on failure; otherwise, it logs a warning.

## Output Schemas & Partitioning

Processed data is saved to a specified output directory (default: `data/`) in **Avro** format.

### 1. Creator Statistics (`creator_stats`)
Aggregated metrics for each creator.

*   **Pathing**: `creator_stats/{YYYY-MM-DD}.avro`
*   **Partitioning**: None (Time-partitioned by file name).
*   **Schema**:
    *   `creator_id`: `long`
    *   `timestamp`: `string` (Date of statistics, YYYY-MM-DD)
    *   `username`: `string`
    *   `follower_count`: `long`
    *   `avg_views`: `double` (Calculated from actual video data)
    *   `top_category`: `string`
    *   `avg_engagement`: `double` ((Likes + Comments + Shares) / Views)
    *   `virality_score`: `double` (Log (engagement / follower_count))
    *   `top_keywords`: `array<string>` (Top 3 keywords from captions)
    *   `updated_at`: `long` (logicalType: timestamp-micros)

### 2. Processed Creators (`creators`)
Cleaned version of the input creators dataset.

*   **Pathing**: `creators/{YYYY-MM-DD}.avro`
*   **Partitioning**: None (Time-partitioned by file name).
*   **Schema**: Mirrors Input Schema (all fields converted to Avro compatible types).

### 3. Processed Videos (`videos`)
Cleaned version of the input videos dataset, organized for efficient retrieval by creator.

*   **Pathing**: `videos/creator_id={creator_id}/{YYYY-MM-DD}.avro`
*   **Partitioning**: **Hive-style partitioning** by `creator_id`.
*   **Schema**: Mirrors Input Schema (all fields converted to Avro compatible types).
