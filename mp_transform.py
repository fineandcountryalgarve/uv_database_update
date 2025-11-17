"""
Transform module for Mailchimp ETL pipeline.
Applies Python-based transformations to raw customer data.
"""
import pandas as pd
import re


def extract_first_names(full_name: str) -> str:
    """
    Extract first name(s) from full name, handling couples and special cases.
    
    Examples:
        "John Smith" -> "John"
        "John & Mary" -> "John & Mary"
        "John and Mary Smith" -> "John & Mary"
        "João e Maria" -> "João & Maria"
        "Jean-Pierre Dubois" -> "Jean-Pierre"
    
    Args:
        full_name: Full name string
        
    Returns:
        str: Extracted and formatted first name(s)
    """
    if not full_name or pd.isna(full_name):
        return ""
    
    # Clean up the name
    name = full_name.strip()
    
    # Normalize couple separators to '&'
    name = re.sub(r'\s+and\s+', ' & ', name, flags=re.IGNORECASE)
    name = re.sub(r'\s+e\s+', ' & ', name, flags=re.IGNORECASE)
    
    # Check if it's a couple (contains &)
    if '&' in name:
        parts = name.split('&')
        if len(parts) == 2:
            # Get first name from each person
            first1 = parts[0].strip().split()[0] if parts[0].strip() else ""
            first2 = parts[1].strip().split()[0] if parts[1].strip() else ""
            
            if first1 and first2:
                return f"{first1.title()} & {first2.title()}"
            elif first1:
                return first1.title()
            elif first2:
                return first2.title()
    
    # Single person - get first name (first word)
    first_name = name.split()[0] if name.split() else ""
    return first_name.title()


def assign_language_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create language-specific first name columns based on 'Speaks' field.
    Uses the actual first name for the spoken language, empty for others.
    
    Args:
        df: DataFrame with 'name' and 'Speaks' columns
        
    Returns:
        DataFrame with language-specific columns added
    """
    # Initialize all language columns as empty
    df['First Name ENG'] = ''
    df['First Name FRE'] = ''
    df['First Name POR'] = ''
    df['First Name GER'] = ''
    
    # Map languages to column names
    language_map = {
        'English': 'First Name ENG',
        'French': 'First Name FRE',
        'Portuguese': 'First Name POR',
        'German': 'First Name GER',
    }
    
    # Assign names to appropriate language column
    for lang, col in language_map.items():
        mask = df['Speaks'] == lang
        df.loc[mask, col] = df.loc[mask, 'name']
    
    # For any language not in the map, use ENG as default
    known_languages = list(language_map.keys())
    mask = ~df['Speaks'].isin(known_languages)
    df.loc[mask, 'First Name ENG'] = df.loc[mask, 'name']
    
    return df


def assign_language_tags(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create language tags based on 'Speaks' field.
    
    Args:
        df: DataFrame with 'Speaks' column
        
    Returns:
        DataFrame with 'Tags' column added
    """
    # Define language to tag mapping
    tag_map = {
        'French': 'FRE',
        'Portuguese': 'POR',
        'German': 'GER',
    }
    
    # Assign tags, default to 'ENG' for unknown languages
    df['Tags'] = df['Speaks'].map(tag_map).fillna('ENG')
    
    return df


def transform_mailchimp_data(df: pd.DataFrame) -> pd.DataFrame | None:
    """
    Transform raw customer data into Mailchimp-ready format.
    
    Transformations include:
    - Extract first names from full names
    - Create language-specific name columns
    - Assign language tags
    - Preserve Client nature from source data
    
    Args:
        df: Raw customer DataFrame from extract step
        
    Returns:
        pd.DataFrame | None: Transformed customer data or None if error
    """
    if df is None or df.empty:
        print("⚠️ No data to transform.")
        return None
    
    try:
        print(f"🔄 Transforming {len(df)} customer records...")
        
        # Create a copy to avoid modifying original
        transformed_df = df.copy()
        
        # 1. Extract first names from Full Name
        transformed_df['name'] = transformed_df['Full Name'].apply(extract_first_names)
        
        # 2. Assign language-specific first name columns
        transformed_df = assign_language_columns(transformed_df)
        
        # 3. Assign language tags
        transformed_df = assign_language_tags(transformed_df)
        
        # 4. Keep Client nature from source (already in the dataframe)
        # Handle any null/missing values in Client nature
        transformed_df['Client nature'] = transformed_df['Client nature'].fillna('buyer')
        
        # Select and reorder final columns for Mailchimp
        final_columns = [
            'Email',
            'name',
            'Speaks',
            'First Name ENG',
            'First Name FRE',
            'First Name POR',
            'First Name GER',
            'Tags',
            'Client nature'
        ]
        
        transformed_df = transformed_df[final_columns]
        
        print(f"✅ Transformed {len(transformed_df)} customer records.")
        
        # Show distribution of Client nature
        print("\n📊 Client nature distribution:")
        print(transformed_df['Client nature'].value_counts().to_string())
        
        # Show sample for verification
        print("\n📋 Sample transformed data:")
        print(transformed_df.head(3).to_string())
        
        return transformed_df
        
    except Exception as e:
        print(f"❌ Error in transform_mailchimp_data: {e}")
        import traceback
        traceback.print_exc()
        return None