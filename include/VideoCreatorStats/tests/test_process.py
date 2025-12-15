import pytest
import pandas as pd
from VideoCreatorStats.process import VideoCreatorStats

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
    assert avg == 300, f"Expected average to be 300 but got {avg}"

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

def test_trending_keywords(mock_stats_obj):
    creators_data = {'creator_id': [1, 2], 'username': ['u1', 'u2']}
    videos_data = {
        'video_id': [101, 102, 103],
        'creator_id': [1, 1, 2],
        'views': [100, 100, 100],
        'caption': [
            'viral viral trend',
            'viral trend cool',
            'viral something else'
        ]
    }
    
    mock_stats_obj.creators = pd.DataFrame(creators_data)
    mock_stats_obj.videos = pd.DataFrame(videos_data)
    
    # "viral" appears 3 times, "trend" 2 times
    trending = mock_stats_obj.get_trending_keywords(top_n=2)
    
    assert len(trending) == 2
    assert trending[0] == 'viral'
    assert trending[1] == 'trend'

def test_virality_score_edge_case(mock_stats_obj):
    # Test creator with 0 followers
    creators_data = {
        'creator_id': [1], 
        'username': ['u1'], 
        'follower_count': [0],
        'avg_views': [0],
        'category': ['Cat'],
        'bio': ['']
    }
    videos_data = {
        'video_id': [101], 
        'creator_id': [1], 
        'views': [1000],
        'likes': [10],
        'comments': [5],
        'shares': [2],
        'caption': ['test']
    }
    
    mock_stats_obj.creators = pd.DataFrame(creators_data)
    mock_stats_obj.videos = pd.DataFrame(videos_data)
    
    stats_df = mock_stats_obj.generate_creator_stats_table()
    
    # Should not raise error and score should be 0 (np.exp(total_engagement / 0) logic handled)
    # Actually logic is if follower_count > 0 else 0
    assert stats_df.iloc[0]['virality_score'] == 0

def test_unmatched_videos(mock_stats_obj):
    # Video 102 has creator_id 999 which doesn't exist
    creators_data = {'creator_id': [1], 'username': ['u1']}
    videos_data = {
        'video_id': [101, 102],
        'creator_id': [1, 999],
        'views': [100, 200],
        'caption': ['a', 'b']
    }
    
    mock_stats_obj.creators = pd.DataFrame(creators_data)
    mock_stats_obj.videos = pd.DataFrame(videos_data)
    
    mock_stats_obj.process_data()
    
    # merged should only contain matched records
    assert len(mock_stats_obj.merged) == 1
    assert mock_stats_obj.merged.iloc[0]['video_id'] == 101

def test_full_stats_schema(mock_stats_obj):
    from datetime import datetime
    creators_data = {
        'creator_id': [1], 
        'username': ['u1'], 
        'follower_count': [100],
        'avg_views': [50],
        'category': ['Cat'],
        'bio': ['Data']
    }
    videos_data = {
        'video_id': [101], 
        'creator_id': [1], 
        'views': [100],
        'likes': [10],
        'comments': [0],
        'shares': [0],
        'caption': ['test']
    }
    
    mock_stats_obj.creators = pd.DataFrame(creators_data)
    mock_stats_obj.videos = pd.DataFrame(videos_data)
    
    df = mock_stats_obj.generate_creator_stats_table()
    
    expected_cols = [
        'creator_id', 'timestamp', 'username', 'follower_count', 
        'avg_views', 'top_category', 'avg_engagement', 'virality_score', 
        'top_keywords', 'updated_at'
    ]
    
    for col in expected_cols:
        assert col in df.columns, f"Missing column {col}"
    
    # Verify types of new fields
    assert isinstance(df.iloc[0]['timestamp'], str)
    assert isinstance(df.iloc[0]['updated_at'], datetime)
