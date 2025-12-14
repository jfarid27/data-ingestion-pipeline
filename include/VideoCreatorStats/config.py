""" Hard coded configuration checks for incoming data.

    This is used to validate the data before processing it. Note each definition,
    must have a col key, and a type key, in an order preserving manner relative to the file.
"""


""" QA Pipeline for creators.csv """
creator_qa_pipeline = [
    {
        "col": 'creator_id',
        "type": "Int64",
        "warnings": [
            {
                "message": "Creator ID is not an integer.",
                "condition": lambda df: df["creator_id"].apply(lambda x: not isinstance(x, int)).any() 
            }
        ],
        "assertions": [
            {
                "condition": lambda df: df["creator_id"].apply(lambda x: x < 0).any(),
                "message": "Creator ID cannot be negative",
                "should_fail": True
            }
        ]
    },
    {
        "col": 'username',
        "type": str,
        "assertions": [
            {
                "message": "Username is not a string.",
                "condition": lambda df: df["username"].apply(lambda x: not isinstance(x, str)).any()
            },
            {
                "condition": lambda df: df["username"].apply(lambda x: x == "").any(),
                "message": "Username cannot be empty",
                "should_fail": True
            }
        ]
    },
    {
        "col": 'follower_count',
        "type": "Int64",
        "fill_na": 0,
        "warnings": [
            {
                "message": "Follower count is not an integer.",
                "condition": lambda df: df["follower_count"].apply(lambda x: not isinstance(x, int)).any()
            }
        ],
        "assertions": [
            {
                "condition": lambda df: df["follower_count"].apply(lambda x: x < 0).any(),
                "message": "Follower count cannot be negative",
                "should_fail": True
            }
        ]
    },
    {
        "col": 'avg_views',
        "type": "Int64",
        "fill_na": 0,
        "warnings": [
            {
                "message": "Average views is not an integer.",
                "condition": lambda df: df["avg_views"].apply(lambda x: not isinstance(x, int)).any()
            }
        ],
        "assertions": [
            {
                "condition": lambda df: df["avg_views"].apply(lambda x: x < 0).any(),
                "message": "Average views cannot be negative",
                "should_fail": True
            }
        ]
    },
    {
        "col": 'category',
        "type": str,
        "fill_na": "",
        "assertions": [
            {
                "message": "Category is not a string.",
                "condition": lambda df: df["category"].apply(lambda x: not isinstance(x, str)).any()
            }
        ]
    },
    {
        "col": 'bio',
        "type": str,
        "fill_na": "",
        "assertions": [
            {
                "message": "Bio is not a string.",
                "condition": lambda df: df["bio"].apply(lambda x: not isinstance(x, str)).any()
            }
        ]
    }
]


""" QA Pipeline for videos.csv """
videos_qa_pipeline = [
    {
        "col": 'video_id',
        "type": str,
        "assertions": [
            {
                "condition": lambda df: df["video_id"].apply(lambda x: not isinstance(x, str)).any(),
                "message": "Video ID is not a string.",
                "should_fail": True
            }
        ]
    },
    {
        "col": 'creator_id',
        "type": "Int64",
        "warnings": [
            {
                "message": "Creator ID is not an integer.",
                "condition": lambda df: df["creator_id"].apply(lambda x: not isinstance(x, int)).any()
            }
        ],
        "assertions": [
            {
                "condition": lambda df: df["creator_id"].apply(lambda x: x < 0).any(),
                "message": "Creator ID cannot be negative",
                "should_fail": True
            }
        ]
    },
    {
        "col": 'views',
        "type": "Int64",
        "fill_na": 0,
        "warnings": [
            {
                "message": "Views is not an integer.",
                "condition": lambda df: df["views"].apply(lambda x: not isinstance(x, int)).any()
            }
        ],
        "assertions": [
            {
                "condition": lambda df: df["views"].apply(lambda x: x < 0).any(),
                "message": "Views cannot be negative",
                "should_fail": True
            }
        ]
    },
    {
        "col": 'likes',
        "type": "Int64",
        "fill_na": 0,
        "warnings": [
            {
                "message": "Likes is not an integer.",
                "condition": lambda df: df["likes"].apply(lambda x: not isinstance(x, int)).any()
            }
        ],
        "assertions": [
            {
                "condition": lambda df: df["likes"].apply(lambda x: x < 0).any(),
                "message": "Likes cannot be negative",
                "should_fail": True
            }
        ]
    },
    {
        "col": 'comments',
        "type": "Int64",
        "fill_na": 0,
        "warnings": [
            {
                "message": "Comments is not an integer.",
                "condition": lambda df: df["comments"].apply(lambda x: not isinstance(x, int)).any()
            }
        ],
        "assertions": [
            {
                "condition": lambda df: df["comments"].apply(lambda x: x < 0).any(),
                "message": "Comments cannot be negative",
                "should_fail": True
            }
        ]
    },
    {
        "col": 'shares',
        "type": "Int64",
        "fill_na": 0,
        "warnings": [
            {
                "message": "Shares is not an integer.",
                "condition": lambda df: df["shares"].apply(lambda x: not isinstance(x, int)).any()
            }
        ],
        "assertions": [
            {
                "condition": lambda df: df["shares"].apply(lambda x: x < 0).any(),
                "message": "Shares cannot be negative",
                "should_fail": True
            }
        ]
    },
    {
        "col": 'caption',
        "type": str,
        "fill_na": "",
        "assertions": [
            {
                "message": "Caption is not a string.",
                "condition": lambda df: df["caption"].apply(lambda x: not isinstance(x, str)).any()
            }
        ]
    }
]