import pandas as pd

# --- 2. THE TRANSFORMATION FUNCTION ---
def transform_csv_to_markdown_report(file_path: str) -> str:
    """
    Reads structured customer review data from a CSV file using Pandas and
    formats it into a clean, human-readable markdown report.
    """
    # Use Pandas to read the structured data
    df = pd.read_csv(file_path)
    
    # Initialize markdown content with a title
    markdown_content = "# Factory Girl Customer Reviews Report\n\n"
    
    # Add summary section
    total_reviews = len(df)
    avg_rating = df['ratingValue'].mean()
    positive_reviews = len(df[df['sentimentAnalysis'] == 'Positive'])
    negative_reviews = len(df[df['sentimentAnalysis'] == 'Negative'])
    
    markdown_content += "## Summary\n\n"
    markdown_content += f"- **Total Reviews:** {total_reviews}\n"
    markdown_content += f"- **Average Rating:** {avg_rating:.1f}/5.0\n"
    markdown_content += f"- **Positive Reviews:** {positive_reviews}\n"
    markdown_content += f"- **Negative Reviews:** {negative_reviews}\n\n"
    
    # Add individual reviews section
    markdown_content += "## Customer Reviews\n\n"
    
    for _, row in df.iterrows():
        # Review header with rating and date
        reviewer = row.get('reviewerTitle', 'Anonymous')
        date = row.get('date', '').split()[0] if isinstance(row.get('date', ''), str) else ''
        rating = int(row.get('ratingValue', 0))
        rating_stars = '★' * rating + '☆' * (5 - rating)
        
        markdown_content += f"### {reviewer} - {rating_stars} - {date}\n\n"
        
        # Review content
        review_text = row.get('ratingText', '')
        if review_text and isinstance(review_text, str):
            markdown_content += f"{review_text}\n\n"
        
        # Review response if available
        reply = row.get('reviewReply', '')
        if reply and isinstance(reply, str):
            markdown_content += f"**Response:**\n\n{reply}\n\n"
        
        # Additional metadata
        sentiment = row.get('sentimentAnalysis', '')
        satisfaction = row.get('satisfactoryLevel', '')
        
        markdown_content += "**Details:**\n\n"
        if sentiment:
            markdown_content += f"- Sentiment: {sentiment}\n"
        if satisfaction:
            markdown_content += f"- Satisfaction Level: {satisfaction}%\n"
        
        markdown_content += "---\n\n"
    
    return markdown_content

# --- 3. EXECUTE THE PIPELINE ---
def main():
    try:
        # Define file paths
        input_csv_path = "/Users/prom1/Documents/sql-agent/con_ai/data/reviews_for_rag.csv"
        output_md_path = "/Users/prom1/Documents/sql-agent/con_ai/data/reviews_for_rag.md"
        
        print(f"Reading CSV from: {input_csv_path}")
        # Check if input file exists
        import os
        if not os.path.exists(input_csv_path):
            print(f"ERROR: Input CSV file not found at {input_csv_path}")
            return
            
        # Check if the directory exists, create it if not
        output_dir = os.path.dirname(output_md_path)
        if not os.path.exists(output_dir):
            print(f"Creating output directory: {output_dir}")
            os.makedirs(output_dir, exist_ok=True)
        
        # Transform CSV to Markdown
        markdown_content = transform_csv_to_markdown_report(input_csv_path)
        
        # Save to file
        print(f"Saving Markdown to: {output_md_path}")
        with open(output_md_path, 'w', encoding='utf-8') as md_file:
            md_file.write(markdown_content)
        print(f"Markdown report saved to {output_md_path}")
        
        # Verify file was created
        if os.path.exists(output_md_path):
            print(f"SUCCESS: File exists at {output_md_path} with size {os.path.getsize(output_md_path)} bytes")
        else:
            print(f"ERROR: File was not created at {output_md_path}")
            
        print("CSV successfully converted to Markdown format.")
    except Exception as e:
        print(f"ERROR: An exception occurred: {e}")
        import traceback
        print(traceback.format_exc())

if __name__ == "__main__":
    main()
