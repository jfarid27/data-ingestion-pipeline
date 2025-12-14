import pytest
import pandas as pd
import sys
import os

# Add parent directory to path to import process
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from process import VideoCreatorStats

@pytest.fixture
def mock_stats_obj(tmp_path):
    # Put dummy paths, we will inject DFs directly
    return VideoCreatorStats("dummy_creators.csv", "dummy_videos.csv")

def test_process_data_failing(mock_stats_obj):
    """
    Test that is designed to fail as requested.
    Checks if the computed average is 0.
    """
    creators_data = {
        'creator_id': [1, 2],
        'username': ['user1', 'user2']
    }
    videos_data = {
        'video_id': [101, 102, 103],
        'creator_id': [1, 1, 2],
        'views': [100, 200, 600],
        'category': ['A', 'A', 'B'],
        'caption': ['a', 'b', 'c']
    }
    
    mock_stats_obj.creators = pd.DataFrame(creators_data)
    mock_stats_obj.videos = pd.DataFrame(videos_data)
    
    # Calculate
    avg = mock_stats_obj.get_average_views_total()
    
    # This assertion is expected to fail
    assert avg == 0, f"Expected average to be 0 but got {avg}"

def test_process_data_correct_logic(mock_stats_obj):
    # Create mock data
    creators_data = {
        'creator_id': [1],
        'username': ['user1']
    }
    videos_data = {
        'video_id': [101, 102],
        'creator_id': [1, 1],
        'views': [100, 200],
        'category': ['Cat1', 'Cat1'],
        'caption': ['Test caption one', 'Test caption two']
    }
    
    mock_stats_obj.creators = pd.DataFrame(creators_data)
    mock_stats_obj.videos = pd.DataFrame(videos_data)
    
    avg = mock_stats_obj.get_average_views_total()
    assert avg == 150

def test_category_stats(mock_stats_obj):
    creators_data = {'creator_id': [1], 'username': ['u1']}
    videos_data = {
        'video_id': [101, 102], 
        'creator_id': [1, 1], 
        'views': [10, 20], 
        'category': ['Gaming', 'Gaming']
    }
    
    mock_stats_obj.creators = pd.DataFrame(creators_data)
    mock_stats_obj.videos = pd.DataFrame(videos_data)
    
    cat_stats = mock_stats_obj.get_views_per_category()
    assert cat_stats['Gaming'] == 30

def test_keywords(mock_stats_obj):
    creators_data = {'creator_id': [1], 'username': ['u1']}
    videos_data = {
        'video_id': [101], 
        'creator_id': [1], 
        'views': [100], 
        # Repeating words to boost tf
        'caption': ['python data python data arbitrary text']
    }
    
    mock_stats_obj.creators = pd.DataFrame(creators_data)
    mock_stats_obj.videos = pd.DataFrame(videos_data)
    
    keywords = mock_stats_obj.get_top_keywords_by_video(top_n=2)
    # Expected output: Check if we got keywords back
    assert 101 in keywords
    # "python" or "data" should be in there
    found_words = keywords[101]
    assert any("python" in w for w in found_words) or any("data" in w for w in found_words)
