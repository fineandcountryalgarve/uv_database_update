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
    Transforms raw customer data from PostgreSQL and merges with pre-enquiries from GSheets.
    
    Args:
        df: Raw customer data (may have pre_enquiries in attrs)
        
    Returns:
        pd.DataFrame: Combined and transformed data ready for Mailchimp
    """
    try:
        # Transform PostgreSQL customers
        transformed_customers = df.copy()
        # ... your existing transformation logic here ...
        
        # Check if we have pre-enquiries to merge
        pre_enquiries = df.attrs.get('pre_enquiries')
        
        if pre_enquiries is not None and not pre_enquiries.empty:
            print(f"\n🔗 Merging {len(pre_enquiries)} pre-enquiries with {len(transformed_customers)} customers")
            
            # Combine both dataframes
            combined_df = pd.concat([transformed_customers, pre_enquiries], ignore_index=True)
            
            # Remove duplicates based on email (keep first occurrence)
            original_count = len(combined_df)
            combined_df = combined_df.drop_duplicates(subset='Email', keep='first')
            duplicates_removed = original_count - len(combined_df)
            
            if duplicates_removed > 0:
                print(f"   🧹 Removed {duplicates_removed} duplicate emails")
            
            # Remove the temporary _source column if it exists
            if '_source' in combined_df.columns:
                combined_df = combined_df.drop(columns=['_source'])
            
            print(f"✅ Final dataset: {len(combined_df)} unique contacts")
            
            return combined_df
        else:
            print("ℹ️ No pre-enquiries to merge")
            return transformed_customers
            
    except Exception as e:
        print(f"❌ Error in transform_mailchimp_data: {e}")
        import traceback
        traceback.print_exc()
        return None