import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer

class TextStatistics:
    """
    Class for performing text analysis and statistics, such as keyword extraction.
    """
    def __init__(self):
        pass

    def extract_keywords_per_item(self, captions, ids, top_n=3):
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

    def get_top_keywords_for_series(self, text_series, top_n=3):
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
            scores = np.asarray(matrix.sum(axis=0)).flatten()
            feature_names = tfidf.get_feature_names_out()
            
            word_scores = list(zip(feature_names, scores))
            word_scores.sort(key=lambda x: x[1], reverse=True)
            return [w for w, s in word_scores[:top_n]]
        except ValueError:
            return []