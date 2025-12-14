"""
Video Creator Stats Processing Module

This module defines the logic for ingesting, cleaning, processing, and analyzing video creator data.
It provides a comprehensive pipeline that:
1.  **Ingests Data**: Loads 'creators' and 'videos' datasets from CSV files.
2.  **Data Cleaning**: Applies Quality Assurance (QA) pipelines (defined in `config.py`) to validate types, handle missing values, and enforce data integrity.
3.  **Data Processing**: Merges datasets, filters unmatched records, and calculates derived metrics.
4.  **Statistical Analysis**: Computes aggregated statistics including:
    - Average views (total and per creator).
    - Engagement rates (likes + comments + shares / views).
    - Category-level performance.
    - Virality scores (light ranking based on MaxEnt model).
5.  **NLP Analysis**: Extracts top keywords from video captions using TF-IDF vectorization to identify content trends for each creator.
6.  **Data Persistence**: Saves the generated statistics and organized raw data into Avro files, partitioned by date and creator ID, suitable for data lakes or downstream analysis.

Usage:
    This script can be executed independently to process data found in the `data/` directory relative to the project root.
    It is also designed to be triggered by orchestration tools like Airflow.
"""
import sys
print("DEBUG: Process script started...", flush=True)

import logging
import pandas as pd
import numpy as np
import os
from sklearn.feature_extraction.text import TfidfVectorizer
import fastavro
from datetime import datetime
from config import creator_qa_pipeline, videos_qa_pipeline
from utils import IngestionFile
from Storage.AvroStorage import AvroPandasStorage

class VideoCreatorStats:
    """
    Class to calculate statistics for video creators based on CSV data inputs.
    """
    def __init__(self, creators_path, videos_path):
        """
        Initialize the VideoCreatorStats with paths to creators and videos CSV files.

        Args:
            creators_path (str): Path to the creators CSV file.
            videos_path (str): Path to the videos CSV file.
        """
        self.creators_path = creators_path
        self.videos_path = videos_path
        self.creators = None
        self.videos = None
        self.merged = None
        self.stats_table = None

    def load_data(self):
        """
        Load creator and video data from CSV files using IngestionFile wrapper.
        Applies cleaning pipelines defined in config.
        """
        self.creators = IngestionFile(creator_qa_pipeline, self.creators_path, 0)
        self.videos = IngestionFile(videos_qa_pipeline, self.videos_path, 0)
        self.creators = self.creators.clean_data()
        self.videos = self.videos.clean_data()

    def process_data(self):
        """
        Merge creators and videos dataframes.
        Filters for videos that have a matching creator in the creators dataset.
        """
        if self.creators is None or self.videos is None:
            self.load_data()
            
        merged_all = pd.merge(self.videos, self.creators, on='creator_id', how='left', indicator=True, suffixes=('', '_creator'))
        
        unmatched_videos = merged_all[merged_all['_merge'] == 'left_only']
        unmatched_count = len(unmatched_videos)
        
        if unmatched_count > 0:
            import logging
            logging.warning(f"Found {unmatched_count} videos with unmatched creator_id")
            
        self.merged = merged_all[merged_all['_merge'] == 'both'].drop(columns=['_merge'])

    def get_average_views_total(self):
        """
        Calculate the average view count across all videos in the merged dataset.

        Returns:
            float: Average number of views.
        """
        if self.merged is None:
            self.process_data()
        if 'views' not in self.merged.columns:
             raise ValueError("Column 'views' not found in merged data")
        return self.merged['views'].mean()

    def get_average_views_by_creator(self):
        """
        Calculate average views for each creator.

        Returns:
            pd.Series: Average views indexed by creator_id.
        """
        if self.merged is None:
            self.process_data()
        return self.merged.groupby('creator_id')['views'].mean()

    def get_views_per_category(self):
        """
        Calculate total views per category.

        Returns:
            pd.Series: Sum of views indexed by category.
        """
        if self.merged is None:
             self.process_data()
        return self.merged.groupby('category')['views'].sum()

    def get_trending_keywords(self, top_n=5):
        """
        Extract top trending keywords from all video captions in the dataset.

        Args:
            top_n (int): Number of top keywords to extract.

        Returns:
            list: List of top keywords.
        """
        if self.merged is None:
            self.process_data()
        
        return self._get_top_keywords_for_text_series(self.merged['caption'], top_n)


    def get_top_keywords_by_video(self, top_n=3):
        """
        Extract top keywords from video captions for each video.

        Args:
            top_n (int): Number of top keywords to extract.

        Returns:
            dict: Mapping of video_id to list of top keywords.
        """
        if self.merged is None:
            self.process_data()
        
        captions = self.merged['caption'].fillna('')
        return self._extract_keywords(captions, self.merged['video_id'], top_n)

    def _extract_keywords(self, captions, ids, top_n=3):
        """
        Helper method to extract top keywords using TF-IDF.

        Args:
            captions (pd.Series): Series of caption text.
            ids (pd.Series): Series of identifiers corresponding to captions.
            top_n (int): Number of top keywords to return per item.

        Returns:
            dict: Mapping of id to list of keywords.
        """
        if captions.empty:
            return {}

        tfidf = TfidfVectorizer(stop_words='english')
        try:
            tfidf_matrix = tfidf.fit_transform(captions)
        except ValueError:
            return {}
            
        feature_names = tfidf.get_feature_names_out()
        results = {}
        
        for i in range(len(captions)):
            id_val = ids.iloc[i]
            row = tfidf_matrix[i]
            _, col_indices = row.nonzero()
            
            word_scores = []
            for col_idx in col_indices:
                word_scores.append((feature_names[col_idx], row[0, col_idx]))
            
            word_scores.sort(key=lambda x: x[1], reverse=True)
            results[id_val] = [w for w, s in word_scores[:top_n]]
            
        return results

    def _get_top_keywords_for_text_series(self, text_series, top_n=3):
        """
        Extract top keywords from a series of text (e.g., all captions for a creator).

        Args:
            text_series (pd.Series): Text data to analyze.
            top_n (int): Number of top keywords to extract.

        Returns:
            list: List of top keywords.
        """
        combined_text = " ".join(text_series.fillna('').astype(str))
        if not combined_text.strip():
            return []
            
        tfidf = TfidfVectorizer(stop_words='english')
        try:
            matrix = tfidf.fit_transform(text_series.fillna(''))
            import numpy as np
            scores = np.asarray(matrix.sum(axis=0)).flatten()
            feature_names = tfidf.get_feature_names_out()
            
            word_scores = list(zip(feature_names, scores))
            word_scores.sort(key=lambda x: x[1], reverse=True)
            return [w for w, s in word_scores[:top_n]]
        except ValueError:
            return []

    def generate_creator_stats_table(self):
        """
        Generate a comprehensive statistics table for all creators.
        Includes metrics like average views, engagement, and top keywords.

        Returns:
            pd.DataFrame: DataFrame containing stats for each creator.
        """
        if self.merged is None:
            self.process_data()

        creators_grp = self.merged.groupby('creator_id')
        
        stats_list = []
        
        for creator_id, group in creators_grp:
            creator_info = self.creators[self.creators['creator_id'] == creator_id].iloc[0]
            
            username = creator_info['username']
            follower_count = creator_info['follower_count']
            top_category = creator_info['category']
            
            total_views = group['views'].sum()
            total_engagement = group['likes'].sum() + group['comments'].sum() + group['shares'].sum()
            
            avg_views = group['views'].mean()
            avg_engagement = total_engagement / total_views if total_views > 0 else 0
            
            virality_score = np.log(total_engagement / follower_count) if follower_count > 0 else 0
            
            top_keywords = self._get_top_keywords_for_text_series(group['caption'], top_n=3)
        
            today_str = datetime.now().strftime('%Y-%m-%d')
            updated_at = datetime.now()
            
            creator_stats = {
                'creator_id': creator_id,
                'timestamp': today_str,
                'username': username,
                'follower_count': follower_count,
                'avg_views': avg_views,
                'top_category': top_category,
                'avg_engagement': avg_engagement,
                'virality_score': virality_score,
                'top_keywords': top_keywords,
                'updated_at': updated_at
            }
            
            top_keywords_str = ",".join(top_keywords)
            
            logging.info(f"Creator stats: {creator_id}|{username}|{follower_count}|{avg_views}|{avg_engagement}|{virality_score}|{top_category}|{top_keywords_str}")
            stats_list.append(creator_stats)
            
        self.stats_table = pd.DataFrame(stats_list)
        return self.stats_table

    def save_data(self, output_dir):
        """
        Save the processed data and statistics to Avro files.

        Args:
            output_dir (str): Directory where output files will be saved.
        """
        if self.stats_table is None:
            self.generate_creator_stats_table()
            
        today_str = datetime.now().strftime('%Y-%m-%d')
        
        stats_dir = os.path.join(output_dir, 'creator_stats')
        os.makedirs(stats_dir, exist_ok=True)
        stats_file = os.path.join(stats_dir, f"{today_str}.avro")
        AvroPandasStorage(stats_file, self.stats_table).save()
        
        creators_out_dir = os.path.join(output_dir, 'creators')
        os.makedirs(creators_out_dir, exist_ok=True)
        creators_file = os.path.join(creators_out_dir, f"{today_str}.avro")
        AvroPandasStorage(creators_file, self.creators).save()
        
        videos_base_dir = os.path.join(output_dir, 'videos')
        
        if self.videos is None:
            self.load_data()
            
        for creator_id, group in self.videos.groupby('creator_id'):
            c_dir = os.path.join(videos_base_dir, f"creator_id={creator_id}")
            os.makedirs(c_dir, exist_ok=True)
            v_file = os.path.join(c_dir, f"{today_str}.avro")
            AvroPandasStorage(v_file, group).save()

    def generate_stats(self):
        """
        Complete pipeline wrapper: load, process, generate stats, and return results.

        Returns:
            dict: Dictionary containing various statistics and the stats table.
        """
        avg_total = self.get_average_views_total()
        avg_by_creator = self.get_average_views_by_creator()
        views_cat = self.get_views_per_category()
        keywords = self.get_top_keywords_by_video()
        top_keywords = self.get_trending_keywords()
        
        self.generate_creator_stats_table()
        
        return {
            "avg_views_total": avg_total,
            "avg_views_by_creator": avg_by_creator,
            "views_per_category": views_cat,
            "top_keywords_by_video": keywords,
            "trending_keywords": top_keywords,
            "creator_stats_table": self.stats_table
        }

import sys

def main():
    
    base_dir = os.path.dirname(os.path.abspath(__file__))
    root_dir = os.path.abspath(os.path.join(base_dir, '../../'))
    data_dir = os.path.join(root_dir, 'data')
    
    creators_path = os.path.join(data_dir, 'creators.csv')
    videos_path = os.path.join(data_dir, 'videos.csv')
    
    logging.info(f"Loading data from {data_dir}")
    
    stats = VideoCreatorStats(creators_path, videos_path)
    results = stats.generate_stats()
    
    logging.info("Saving data...")
    stats.save_data(data_dir)
    logging.info("Data saved successfully.")
    
    logging.info(f"Average Views Total: {results['avg_views_total']}")
    logging.info(f"Average Views By Creator: {results['avg_views_by_creator']}")
    logging.info(f"Views Per Category: {results['views_per_category']}")
    logging.info(f"Top Keywords By Video: {results['top_keywords_by_video']}")
    logging.info(f"Trending Keywords: {results['trending_keywords']}")

if __name__ == "__main__":
    logging.basicConfig(
        force=True,
        stream=sys.stdout,
        format="%(asctime)s - %(levelname)s - %(message)s",
        style="%",
        datefmt="%Y-%m-%d %H:%M",
        level=logging.DEBUG
    )
    main()
